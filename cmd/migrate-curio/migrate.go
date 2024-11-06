package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	transportTypes "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/curio/deps"
	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	vfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/context"
	"golang.org/x/xerrors"
)

var migrateCmd = &cli.Command{
	Name:        "migrate",
	Description: "Migrate boost metadata to Curio",
	Usage:       "migrate-curio migrate",
	Before:      before,
	Action: func(cctx *cli.Context) error {
		repoDir, err := homedir.Expand(cctx.String(FlagBoostRepo))
		if err != nil {
			return err
		}

		return migrate(cctx, repoDir)
	},
}

func migrate(cctx *cli.Context, repoDir string) error {
	ctx := cctx.Context

	r, err := lotus_repo.NewFS(repoDir)
	if err != nil {
		return err
	}
	ok, err := r.Exists()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("repo at '%s' is not initialized", cctx.String(FlagBoostRepo))
	}

	lr, err := r.Lock(repo.Boost)
	if err != nil {
		return err
	}

	mds, err := lr.Datastore(ctx, "/metadata")
	if err != nil {
		return err
	}

	maddrb, err := mds.Get(context.TODO(), datastore.NewKey("miner-address"))
	if err != nil {
		return err
	}

	maddr, err := address.NewFromBytes(maddrb)
	if err != nil {
		return err
	}

	// Connect to full Node
	full, closer, err := lcli.GetFullNodeAPIV1(cctx)
	if err != nil {
		return xerrors.Errorf("failed to connect to full node API: %w", err)
	}
	defer closer()

	// Connect to Harmony DB
	hdb, err := deps.MakeDB(cctx)
	if err != nil {
		return xerrors.Errorf("failed to connect to harmony DB: %w", err)
	}

	dbPath := path.Join(repoDir, "boost.db?cache=shared")
	sqldb, err := db.SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("opening boost sqlite db: %w", err)
	}

	mdbPath := path.Join(repoDir, "migrate-curio.db?cache=shared")
	mdb, err := db.SqlDB(mdbPath)
	if err != nil {
		return fmt.Errorf("opening migrate sqlite db: %w", err)
	}

	_, err = mdb.Exec(`CREATE TABLE IF NOT EXISTS Deals (
							ID TEXT UNIQUE,
							DB BOOL NOT NULL DEFAULT FALSE,
							LID BOOL NOT NULL DEFAULT FALSE,
							Pipeline BOOL NOT NULL DEFAULT FALSE
						);`)
	if err != nil {
		return fmt.Errorf("failed to create migration table: %w", err)
	}

	mActor, err := full.StateGetActor(ctx, maddr, ltypes.EmptyTSK)
	if err != nil {
		return fmt.Errorf("getting actor for the miner %s: %w", maddr, err)
	}
	astore := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(full)))
	mas, err := miner.Load(astore, mActor)
	if err != nil {
		return fmt.Errorf("loading miner actor state %s: %w", maddr, err)
	}
	liveSectors, err := miner.AllPartSectors(mas, miner.Partition.LiveSectors)
	if err != nil {
		return fmt.Errorf("getting live sector sets for miner %s: %w", maddr, err)
	}
	unProvenSectors, err := miner.AllPartSectors(mas, miner.Partition.UnprovenSectors)
	if err != nil {
		return fmt.Errorf("getting unproven sector sets for miner %s: %w", maddr, err)
	}
	activeSectors, err := bitfield.MergeBitFields(liveSectors, unProvenSectors)
	if err != nil {
		return fmt.Errorf("merging bitfields to generate all sealed sectors on miner %s: %w", maddr, err)
	}

	// Migrate Boost deals
	if err := migrateBoostDeals(ctx, activeSectors, maddr, hdb, sqldb, mdb); err != nil {
		return xerrors.Errorf("failed to migrate boost deals: %w", err)
	}

	// Migrate Legacy deal
	if err := migrateLegacyDeals(ctx, full, activeSectors, maddr, hdb, mds, mdb); err != nil {
		return xerrors.Errorf("failed to migrate legacy deals: %w", err)
	}

	// Migrate Direct deals
	if err := migrateDDODeals(ctx, full, activeSectors, maddr, hdb, sqldb, mdb); err != nil {
		return xerrors.Errorf("failed to migrate DDO deals: %w", err)
	}

	return nil
}

func migrateBoostDeals(ctx context.Context, activeSectors bitfield.BitField, maddr address.Address, hdb *harmonydb.DB, sqldb, mdb *sql.DB) error {
	sdb := db.NewDealsDB(sqldb)

	mid, err := address.IDFromAddress(maddr)
	if err != nil {
		return fmt.Errorf("address.IDFromAddress: %s", err)
	}

	aDeals, err := sdb.ListActive(ctx)
	if err != nil {
		return err
	}

	cDeals, err := sdb.ListCompleted(ctx)
	if err != nil {
		return err
	}

	deals := append(aDeals, cDeals...)

	for i, deal := range deals {
		if i > 0 && i%100 == 0 {
			fmt.Printf("Migrating Boost Deals: %d / %d (%0.2f%%)\n", i, len(deals), float64(i)/float64(len(deals))*100)
		}

		llog := log.With("Boost Deal", deal.DealUuid.String())
		// Skip deals which are before add piece
		if deal.Checkpoint < dealcheckpoints.AddedPiece {
			llog.Infow("Skipping as checkpoint is below add piece")
			continue
		}

		// Skip deals which do not have chain deal ID
		if deal.ChainDealID == 0 {
			llog.Infow("Skipping as chain deal ID is 0")
			continue
		}

		// Skip deals which do not have retryable error
		if deal.Retry == types.DealRetryFatal {
			llog.Infow("Skipping as deal retry is fatal")
			continue
		}

		// SKip sector 0. This might cause some deals to not migrate but
		// that is better than migrating faulty deals
		if deal.SectorID == 0 {
			llog.Infow("Skipping as sector ID is 0")
			continue
		}

		// Skip if the sector for the deal is not alive
		ok, err := activeSectors.IsSet(uint64(deal.SectorID))
		if err != nil {
			return err
		}
		if !ok {
			llog.Infof("Skipping as sector %d is not alive anymore", deal.SectorID)
			continue
		}

		// Skip if already migrated
		var a, b, c bool
		err = mdb.QueryRow(`SELECT DB, LID, Pipeline FROM Deals WHERE ID = ?`, deal.DealUuid.String()).Scan(&a, &b, &c)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("deal: %s: failed to check migration status: %w", deal.DealUuid.String(), err)
			}
		}
		if a && b && c {
			llog.Infow("Skipped as this deal is already migrated")
			continue
		}

		propJson, err := json.Marshal(deal.ClientDealProposal.Proposal)
		if err != nil {
			return fmt.Errorf("deal: %s: json.Marshal(piece.DealProposal): %s", deal.DealUuid.String(), err)
		}

		sigByte, err := deal.ClientDealProposal.ClientSignature.MarshalBinary()
		if err != nil {
			return fmt.Errorf("deal: %s: marshal client signature: %s", deal.DealUuid.String(), err)
		}

		prop := deal.ClientDealProposal.Proposal

		sProp, err := deal.SignedProposalCid()
		if err != nil {
			return err
		}

		// de-serialize transport opaque token
		tInfo := &transportTypes.HttpRequest{}
		if err := json.Unmarshal(deal.Transfer.Params, tInfo); err != nil {
			return fmt.Errorf("deal: %s: failed to de-serialize transport params bytes '%s': %s", deal.DealUuid.String(), string(deal.Transfer.Params), err)
		}

		headers, err := json.Marshal(tInfo.Headers)
		if err != nil {
			return fmt.Errorf("deal: %s: failed to marshal headers: %s", deal.DealUuid.String(), err)
		}

		// Cbor marshal the Deal Label manually as non-string label will result in "" with JSON marshal
		label := prop.Label
		buf := new(bytes.Buffer)
		err = label.MarshalCBOR(buf)
		if err != nil {
			return fmt.Errorf("cbor marshal label: %w", err)

		}

		_, err = hdb.BeginTransaction(ctx, func(tx *harmonydb.Tx) (bool, error) {
			// Add deal to HarmonyDB
			if !a {
				_, err = tx.Exec(`INSERT INTO market_mk12_deals (uuid, sp_id, signed_proposal_cid, 
                                proposal_signature, proposal, piece_cid, 
                                piece_size, offline, verified, start_epoch, end_epoch, 
                                client_peer_id, fast_retrieval, announce_to_ipni, url, url_headers, chain_deal_id, publish_cid, created_at, label) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
				ON CONFLICT (uuid) DO NOTHING`,
					deal.DealUuid.String(), mid, sProp.String(), sigByte, propJson, prop.PieceCID.String(),
					prop.PieceSize, deal.IsOffline, prop.VerifiedDeal, prop.StartEpoch, prop.EndEpoch, deal.ClientPeerID.String(),
					deal.FastRetrieval, deal.AnnounceToIPNI, tInfo.URL, headers, int64(deal.ChainDealID), deal.PublishCID.String(), deal.CreatedAt, buf.Bytes())

				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to add the deal to harmonyDB: %w", deal.DealUuid.String(), err)
				}

				// Mark deal added to harmonyDB
				_, err = mdb.Exec(`INSERT INTO Deals (ID, DB, LID) VALUES (?, TRUE, FALSE) ON CONFLICT(ID) DO NOTHING`, deal.DealUuid.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal migrated: %w", deal.DealUuid.String(), err)
				}
			}

			if !b {
				// Add LID details to pieceDeal in HarmonyDB
				_, err = tx.Exec(`SELECT process_piece_deal($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
					deal.DealUuid.String(), prop.PieceCID.String(), true, mid, deal.SectorID, deal.Offset,
					prop.PieceSize, deal.NBytesReceived, false, false, deal.ChainDealID)
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to update piece metadata and piece deal: %w", deal.DealUuid.String(), err)
				}

				// Mark deal added to pieceDeal in HarmonyDB
				_, err = mdb.Exec(`UPDATE Deals SET LID = TRUE WHERE ID = ?`, deal.DealUuid.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal LID migrated: %w", deal.DealUuid.String(), err)
				}
			}

			if !c {
				// Check if we can index and announce i.e. we have unsealed copy
				var exists bool
				err = tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM sector_location WHERE miner_id = $1
              							AND sector_num = $2
              							AND sector_filetype = 1);`, mid, deal.SectorID).Scan(&exists)
				if err != nil {
					return false, fmt.Errorf("seal: %s: failed to check if sector is unsealed: %w", deal.DealUuid.String(), err)
				}

				if exists {
					var proof abi.RegisteredSealProof
					err = tx.QueryRow(`SELECT reg_seal_proof FROM sectors_meta WHERE sp_id = $1 AND sector_num = $2`, mid, deal.SectorID).Scan(&proof)
					if err != nil {
						llog.Errorw("failed to get sector proof", "error", err, "deal", deal.DealUuid.String(), "sector", deal.SectorID, "miner", mid)
						return false, nil
					}

					// Add deal to mk12 pipeline in Curio for indexing and announcement
					_, err = tx.Exec(`INSERT INTO market_mk12_deal_pipeline_migration (
									uuid, sp_id, piece_cid, piece_size, raw_size, sector, reg_seal_proof, sector_offset, should_announce
								) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (uuid) DO NOTHING`,
						deal.DealUuid.String(), mid, prop.PieceCID.String(), prop.PieceSize, deal.NBytesReceived,
						deal.SectorID, proof, deal.Offset, deal.AnnounceToIPNI)
					if err != nil {
						return false, fmt.Errorf("deal: %s: failed to add deal to pipeline for indexing and announcing: %w", deal.DealUuid.String(), err)
					}
				} else {
					llog.Infof("Skipping indexing as sector %d is not unsealed", deal.SectorID)
				}
			}
			return true, nil
		}, harmonydb.OptionRetry())
		if err != nil {
			return err
		}

	}

	return nil
}

func migrateLegacyDeals(ctx context.Context, full v1api.FullNode, activeSectors bitfield.BitField, maddr address.Address, hdb *harmonydb.DB, ds datastore.Batching, mdb *sql.DB) error {
	mid, err := address.IDFromAddress(maddr)
	if err != nil {
		return fmt.Errorf("address.IDFromAddress: %s", err)
	}

	// Get the deals FSM
	provDS := namespace.Wrap(ds, datastore.NewKey("/deals/provider"))
	deals, migrate, err := vfsm.NewVersionedFSM(provDS, fsm.Parameters{
		StateType:     legacytypes.MinerDeal{},
		StateKeyField: "State",
	}, nil, "2")
	if err != nil {
		return fmt.Errorf("reading legacy deals from datastore: %w", err)
	}

	err = migrate(ctx)
	if err != nil {
		return fmt.Errorf("running provider fsm migration script: %w", err)
	}

	lm := legacy.NewLegacyDealsManager(deals)
	go lm.Run(ctx)
	// Wait for 5 seconds
	time.Sleep(time.Second * 5)

	legacyDeals, err := lm.ListDeals()
	if err != nil {
		return fmt.Errorf("getting legacy deals: %w", err)
	}

	head, err := full.ChainHead(ctx)
	if err != nil {
		return err
	}

	for i, deal := range legacyDeals {
		if i > 0 && i%100 == 0 {
			fmt.Printf("Migrating Legacy Deals: %d / %d (%0.2f%%)\n", i, len(legacyDeals), float64(i)/float64(len(legacyDeals))*100)
		}
		llog := log.With("Boost Deal", deal.ProposalCid.String())
		// Skip deals which do not have chain deal ID
		if deal.DealID == 0 {
			llog.Infow("Skipping as chain deal ID is 0")
			continue
		}

		// SKip sector 0. This might cause some deals to not migrate but
		// that is better than migrating faulty deals
		if deal.SectorNumber == 0 {
			llog.Infow("Skipping as sector ID is 0")
			continue
		}

		// Skip expired legacy deals
		if deal.ClientDealProposal.Proposal.EndEpoch < head.Height() {
			llog.Infow("Deal end epoch is lower than current height")
			continue
		}

		// Skip if the sector for the deal is not alive
		ok, err := activeSectors.IsSet(uint64(deal.SectorNumber))
		if err != nil {
			return err
		}
		if !ok {
			llog.Infof("Skipping as sector %d is not alive anymore", deal.SectorNumber)
			continue
		}

		// Skip if already migrated
		var a, b, c bool
		err = mdb.QueryRow(`SELECT DB, LID, Pipeline FROM Deals WHERE ID = ?`, deal.ProposalCid.String()).Scan(&a, &b, &c)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("deal: %s: failed to check migration status: %w", deal.ProposalCid.String(), err)
			}
		}
		if a && b && c {
			llog.Infow("Skipped as this deal is already migrated")
			continue
		}

		propJson, err := json.Marshal(deal.ClientDealProposal.Proposal)
		if err != nil {
			return fmt.Errorf("deal: %s: json.Marshal(piece.DealProposal): %s", deal.ProposalCid.String(), err)
		}

		sigByte, err := deal.ClientDealProposal.ClientSignature.MarshalBinary()
		if err != nil {
			return fmt.Errorf("deal: %s: marshal client signature: %s", deal.ProposalCid.String(), err)
		}

		prop := deal.ClientDealProposal.Proposal

		_, err = hdb.Exec(ctx, `INSERT INTO signed_proposal_cid (signed_proposal_cid, sp_id, client_peer_id,
                                proposal_signature, proposal, piece_cid, 
                                piece_size, verified, start_epoch, end_epoch, 
                                publish_cid, chain_deal_id, fast_retrieval, created_at, sector_num) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
				ON CONFLICT (signed_proposal_cid) DO NOTHING`,
			deal.ProposalCid, mid, deal.Client.String(), sigByte, propJson, prop.PieceCID.String(),
			prop.PieceSize, prop.VerifiedDeal, prop.StartEpoch, prop.EndEpoch, deal.PublishCid.String(),
			deal.FastRetrieval, deal.CreationTime, deal.SectorNumber)

		if err != nil {
			return fmt.Errorf("deal: %s: failed to add the legacy deal to harmonyDB: %w", deal.ProposalCid.String(), err)
		}

		// Mark deal added to harmonyDB
		_, err = mdb.Exec(`INSERT INTO Deals (ID, DB, LID) VALUES (?, TRUE, TRUE) ON CONFLICT(ID) DO NOTHING`, deal.ProposalCid.String())
		if err != nil {
			return fmt.Errorf("deal: %s: failed to mark deal migrated: %w", deal.ProposalCid.String(), err)
		}
	}

	return nil
}

func migrateDDODeals(ctx context.Context, full v1api.FullNode, activeSectors bitfield.BitField, maddr address.Address, hdb *harmonydb.DB, sqldb, mdb *sql.DB) error {
	ddb := db.NewDirectDealsDB(sqldb)

	mid, err := address.IDFromAddress(maddr)
	if err != nil {
		return fmt.Errorf("address.IDFromAddress: %s", err)
	}

	deals, err := ddb.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all DDO deals: %w", err)
	}

	for i, deal := range deals {
		if i > 0 && i%100 == 0 {
			fmt.Printf("Migrating DDO Deals: %d / %d (%0.2f%%)\n", i, len(deals), float64(i)/float64(len(deals))*100)
		}
		llog := log.With("DDO Deal", deal.ID.String())
		if deal.Err != "" && deal.Retry == types.DealRetryFatal {
			llog.Infow("Skipping as deal retry is fatal")
			continue
		}

		if deal.Checkpoint < dealcheckpoints.AddedPiece {
			llog.Infow("Skipping as checkpoint is below add piece")
			continue
		}

		claim, err := full.StateGetClaim(ctx, maddr, verifreg9types.ClaimId(deal.AllocationID), ltypes.EmptyTSK)
		if err != nil {
			return fmt.Errorf("deal: %s: error getting the claim status: %w", deal.ID.String(), err)
		}
		if claim == nil {
			llog.Infow("Skipping as checkpoint is below add piece")
			continue
		}
		if claim.Sector != deal.SectorID {
			return fmt.Errorf("deal: %s: sector mismatch for deal", deal.ID.String())
		}

		// Skip if the sector for the deal is not alive
		ok, err := activeSectors.IsSet(uint64(deal.SectorID))
		if err != nil {
			return err
		}
		if !ok {
			llog.Infow("Skipping as sector ID is 0")
			continue
		}

		// Skip if already migrated
		var a, b, c bool
		err = mdb.QueryRow(`SELECT DB, LID, Pipeline FROM Deals WHERE ID = ?`, deal.ID.String()).Scan(&a, &b, &c)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("deal: %s: failed to check migration status: %w", deal.ID.String(), err)
			}
		}
		if a && b && c {
			continue
		}

		_, err = hdb.BeginTransaction(ctx, func(tx *harmonydb.Tx) (bool, error) {
			if !a {
				// Add DDO deal to harmonyDB
				_, err = tx.Exec(`INSERT INTO market_direct_deals (uuid, sp_id, created_at, client, offline, verified,
                                  start_epoch, end_epoch, allocation_id, piece_cid, piece_size, fast_retrieval, announce_to_ipni) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (uuid) DO NOTHING`,
					deal.ID.String(), mid, deal.CreatedAt, deal.Client.String(), true, true, deal.StartEpoch, deal.EndEpoch, deal.AllocationID,
					deal.PieceCID.String(), deal.PieceSize, true, true)

				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to add the DDO deal to harmonyDB: %w", deal.ID.String(), err)
				}

				// Mark deal added to harmonyDB
				_, err = mdb.Exec(`INSERT INTO Deals (ID, DB, LID) VALUES (?, TRUE, FALSE) ON CONFLICT(ID) DO NOTHING`, deal.ID.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark DDO deal migrated: %w", deal.ID.String(), err)
				}
			}

			if !b {
				// Add LID details to pieceDeal in HarmonyDB
				_, err = tx.Exec(`SELECT process_piece_deal($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
					deal.ID.String(), deal.PieceCID.String(), false, mid, deal.SectorID, deal.Offset, deal.PieceSize, deal.InboundFileSize, false)
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to update piece metadata and piece deal for DDO deal %s: %w", deal.ID.String(), deal.ID.String(), err)
				}

				// Mark deal added to pieceDeal in HarmonyDB
				_, err = mdb.Exec(`UPDATE Deals SET LID = TRUE WHERE ID = ?`, deal.ID.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal LID migrated: %w", deal.ID.String(), err)
				}
			}

			// TODO: Confirm if using the mk12 pipeline will have any impact for DDO deals
			if !c {
				// Check if we can index and announce i.e. we have unsealed copy
				var exists bool
				err = tx.QueryRow(`SELECT EXISTS (SELECT 1 FROM sector_location WHERE miner_id = $1
              							AND sector_num = $2
              							AND sector_filetype = 1);`, mid, deal.SectorID).Scan(&exists)
				if err != nil {
					return false, fmt.Errorf("seal: %s: failed to check if sector is unsealed: %w", deal.ID.String(), err)
				}

				if exists {
					var proof abi.RegisteredSealProof
					err = tx.QueryRow(`SELECT reg_seal_proof FROM sectors_meta WHERE sp_id = $1 AND sector_num = $2`, mid, deal.SectorID).Scan(&proof)
					if err != nil {
						return false, fmt.Errorf("deal: %s: failed to get sector proof: %w", deal.ID.String(), err)
					}

					// Add deal to mk12 pipeline in Curio for indexing and announcement
					_, err = tx.Exec(`INSERT INTO market_mk12_deal_pipeline_migration (
									uuid, sp_id, piece_cid, piece_size, raw_size, sector, reg_seal_proof, sector_offset, should_announce
								) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (uuid) DO NOTHING`,
						deal.ID.String(), mid, deal.PieceCID.String(), deal.PieceSize, deal.InboundFileSize,
						deal.SectorID, proof, deal.Offset, true)
					if err != nil {
						return false, fmt.Errorf("deal: %s: failed to add DDO deal to pipeline for indexing and announcing: %w", deal.ID.String(), err)
					}
				} else {
					llog.Infof("Skipping indexing as sector %d is not unsealed", deal.SectorID)
				}
			}
			return true, nil
		}, harmonydb.OptionRetry())
		if err != nil {
			return err
		}
	}

	return nil
}
