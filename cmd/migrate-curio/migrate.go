package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	vfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-statemachine/fsm"

	"github.com/filecoin-project/boost/db"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	transportTypes "github.com/filecoin-project/boost/transport/types"

	"github.com/filecoin-project/curio/harmony/harmonydb"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
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
	hdb, err := makeDB(cctx)
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

	cl := bdclient.NewStore()
	defer cl.Close(ctx)
	err = cl.Dial(ctx, cctx.String("api-lid"))
	if err != nil {
		return fmt.Errorf("connecting to local index directory service: %w", err)
	}

	_, err = cl.ListPieces(ctx)
	if err != nil {
		return fmt.Errorf("listing pieces from local index directory service: %w", err)
	}

	// Migrate Boost deals
	if err := migrateBoostDeals(ctx, activeSectors, maddr, hdb, sqldb, mdb, cl); err != nil {
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

func migrateBoostDeals(ctx context.Context, activeSectors bitfield.BitField, maddr address.Address, hdb *harmonydb.DB, sqldb, mdb *sql.DB, bdclient *bdclient.Store) error {
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

		tInfo := &transportTypes.HttpRequest{}
		var headers []byte
		if !deal.IsOffline {
			// de-serialize transport opaque token
			if err := json.Unmarshal(deal.Transfer.Params, tInfo); err != nil {
				return fmt.Errorf("deal: %s: failed to de-serialize transport params bytes '%s': %s", deal.DealUuid.String(), string(deal.Transfer.Params), err)
			}

			goheaders := http.Header{}
			for k, v := range tInfo.Headers {
				goheaders.Set(k, v)
			}

			headers, err = json.Marshal(goheaders)
			if err != nil {
				return fmt.Errorf("deal: %s: failed to marshal headers: %s", deal.DealUuid.String(), err)
			}
		} else {
			headers = []byte("{}")
		}

		// Cbor marshal the Deal Label manually as non-string label will result in "" with JSON marshal
		label := prop.Label
		propCid, err := prop.Cid()
		if err != nil {
			return fmt.Errorf("deal: %s: failed to calculate prop.Cid: %s", deal.DealUuid.String(), err)
		}
		buf := new(bytes.Buffer)
		err = label.MarshalCBOR(buf)
		if err != nil {
			return fmt.Errorf("cbor marshal label: %w", err)

		}

		if deal.NBytesReceived == 0 {
			if deal.Transfer.Size > 0 {
				deal.NBytesReceived = int64(deal.Transfer.Size)
			} else {
				pds, err := bdclient.GetPieceDeals(ctx, prop.PieceCID)
				if err != nil {
					return fmt.Errorf("failed to get piece deals from LID: %w", err)
				}
				for _, pd := range pds {
					if pd.CarLength > 0 {
						deal.NBytesReceived = int64(pd.CarLength)
						break
					}
				}
			}
		}

		if deal.NBytesReceived == 0 {
			return fmt.Errorf("deal: %s: failed to get raw size from SQL or LID", deal.DealUuid.String())
		}

		stx, err := mdb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		_, err = hdb.BeginTransaction(ctx, func(tx *harmonydb.Tx) (bool, error) {
			// Add deal to HarmonyDB
			if !a {
				_, err = tx.Exec(`INSERT INTO market_mk12_deals (uuid, sp_id, signed_proposal_cid, 
                                proposal_signature, proposal, proposal_cid, piece_cid, 
                                piece_size, raw_size, offline, verified, start_epoch, end_epoch, 
                                client_peer_id, fast_retrieval, announce_to_ipni, url, url_headers, chain_deal_id, publish_cid, created_at, label) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
				ON CONFLICT (uuid) DO NOTHING`,
					deal.DealUuid.String(), mid, sProp.String(), sigByte, propJson, propCid, prop.PieceCID.String(),
					prop.PieceSize, deal.NBytesReceived, deal.IsOffline, prop.VerifiedDeal, prop.StartEpoch, prop.EndEpoch, deal.ClientPeerID.String(),
					deal.FastRetrieval, deal.AnnounceToIPNI, tInfo.URL, headers, int64(deal.ChainDealID), deal.PublishCID.String(), deal.CreatedAt, buf.Bytes())

				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to add the deal to harmonyDB: %w", deal.DealUuid.String(), err)
				}

				// Mark deal added to harmonyDB
				_, err = stx.Exec(`INSERT INTO Deals (ID, DB, LID) VALUES (?, TRUE, FALSE) ON CONFLICT(ID) DO NOTHING`, deal.DealUuid.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal migrated: %w", deal.DealUuid.String(), err)
				}
			}

			if !b {
				// Add LID details to pieceDeal in HarmonyDB
				_, err = tx.Exec(`SELECT process_piece_deal($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
					deal.DealUuid.String(), prop.PieceCID.String(), true, mid, deal.SectorID, deal.Offset,
					prop.PieceSize, deal.NBytesReceived, false, nil, false, deal.ChainDealID)
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to update piece metadata and piece deal: %w", deal.DealUuid.String(), err)
				}

				// Mark deal added to pieceDeal in HarmonyDB
				_, err = stx.Exec(`UPDATE Deals SET LID = TRUE WHERE ID = ?`, deal.DealUuid.String())
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

				// Mark deal added to pipeline in HarmonyDB
				_, err = stx.Exec(`UPDATE Deals SET Pipeline = TRUE WHERE ID = ?`, deal.DealUuid.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal Pipeline migrated: %w", deal.DealUuid.String(), err)
				}
			}
			return true, nil
		}, harmonydb.OptionRetry())
		if err != nil {
			serr := stx.Rollback()
			if serr != nil {
				return fmt.Errorf("failed to commit Haromy changes: %w and failed to rollback transaction: %w", err, serr)
			}
			return fmt.Errorf("failed to commit Haromy changes: %w", err)
		}
		err = stx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit Haromy changes: %w", err)
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
		llog := log.With("Legacy Deal", deal.ProposalCid.String())
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
		if deal.Proposal.EndEpoch < head.Height() {
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

		propJson, err := json.Marshal(deal.Proposal)
		if err != nil {
			return fmt.Errorf("deal: %s: json.Marshal(piece.DealProposal): %s", deal.ProposalCid.String(), err)
		}

		sigByte, err := deal.ClientSignature.MarshalBinary()
		if err != nil {
			return fmt.Errorf("deal: %s: marshal client signature: %s", deal.ProposalCid.String(), err)
		}

		prop := deal.Proposal

		_, err = hdb.Exec(ctx, `INSERT INTO market_legacy_deals (signed_proposal_cid, sp_id, client_peer_id,
                                proposal_signature, proposal, piece_cid, 
                                piece_size, verified, start_epoch, end_epoch, 
                                publish_cid, chain_deal_id, fast_retrieval, created_at, sector_num) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
				ON CONFLICT (sp_id, piece_cid, signed_proposal_cid) DO NOTHING`,
			deal.ProposalCid, mid, deal.Client.String(), sigByte, propJson, prop.PieceCID.String(),
			prop.PieceSize, prop.VerifiedDeal, prop.StartEpoch, prop.EndEpoch, deal.PublishCid.String(),
			deal.DealID, deal.FastRetrieval, deal.CreationTime.Time(), deal.SectorNumber)

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

// legacyNoClaimErr is the sentence a released Boost stored in direct_deals.error
// when it gave up looking for a deal's claim. It is matched exactly, and the
// other fatal sentences are deliberately not: "sector mismatch for claim" is
// only written once a claim was found and placed in the wrong sector, which
// needs a claim to exist, so it cannot stand in for this case.
const legacyNoClaimErr = "no claim found"

// fatalSkip reports whether a deal's recorded error is a verdict the migration
// has to respect, rather than one it can decide again for itself.
//
// Only the no-claim sentence is given up on: past nv29 FIP-0118 writes no claim
// at all, so a released Boost reached it on every post-nv29 deal that got as far
// as sealing, and the deal's own sector is the evidence that replaced the claim.
// Every other fatal error stands -- it says something the chain check below
// cannot see, and re-deciding those rows would migrate deals Boost rejected for
// reasons that still hold.
func fatalSkip(err string, retry types.DealRetryType) bool {
	return err != "" && retry == types.DealRetryFatal && err != legacyNoClaimErr
}

// ddoOnChainResult is the three-way answer to "did this DDO deal's data land in
// its sector". It is a result type rather than a bool plus error because
// ddoNoPieceSource is neither a verdict nor a contradiction: the run has no way
// to read the sector's pieces, so it cannot decide, and the caller has to count
// it and move on rather than abort.
type ddoOnChainResult int

const (
	// ddoNotOnChain: the evidence was read and does not place our piece.
	ddoNotOnChain ddoOnChainResult = iota
	// ddoOnChain: the deal's data is in the sector.
	ddoOnChain
	// ddoNoPieceSource: no claim, the sector carries data, and the pieces gave no
	// answer -- either the list could not be read or it came back empty, which a
	// sector that carries data cannot be. "Our piece is in there" is unproven
	// either way, so nothing is migrated.
	ddoNoPieceSource
)

// ddoDealOnChain reports whether a DDO deal recorded in boost's DB still has
// evidence of itself in the sector it belongs to, which is what earns it a place
// in the migration.
//
// The evidence differs by era. Before nv29 the chain's verifreg claim is the
// proof, and a claim pointing at another sector is a contradiction rather than
// evidence for this one -- it cannot be repaired here, so it is an error. From
// nv29 on FIP-0118 writes no claim at all, so a post-nv29 deal has none by
// construction and its sector answers instead; reading that absent claim as "the
// deal did not land" would drop every post-nv29 direct deal from the migration.
//
// The sector's weight alone is not enough to answer with. It is a property of
// the whole sector, and a sector can hold several pieces from several deals, so
// it says "something landed here", never "this deal landed here". The case that
// shows the gap is a snap that aborted and was followed by another piece
// entering the same sector: the sector carries data, our piece is not in it, and
// recovering the deal would hand a client data that was never sealed. So when
// there is no claim the piece list has to be read, and our (PieceCID, Size,
// offset) has to appear in it.
//
// pieces and piecesKnown come from the sealer's recorded piece list. When that
// list gives no answer the result is ddoNoPieceSource, never a guess -- including
// when it comes back empty for a sector that carries data, because such a sector
// has pieces by definition, so an empty list means the list was not written (the
// migration ran before `curio guided-setup`) rather than that our piece is
// absent. Reading that emptiness as absence would drop every post-nv29 deal in
// silence, which is the failure this whole check exists to avoid.
func ddoDealOnChain(claim *verifreg9types.Claim, si *miner.SectorOnChainInfo, pieces []lapi.SectorPiece, piecesKnown bool, deal *types.DirectDeal) (ddoOnChainResult, error) {
	if claim != nil {
		if claim.Sector != deal.SectorID {
			return ddoNotOnChain, errors.New("sector mismatch for deal")
		}
		return ddoOnChain, nil
	}
	if !sectorstatemgr.SectorCarriesData(si) {
		return ddoNotOnChain, nil
	}
	if !piecesKnown || len(pieces) == 0 {
		return ddoNoPieceSource, nil
	}
	if !sectorstatemgr.PieceInSector(pieces, deal.PieceCID, deal.PieceSize, deal.Offset) {
		return ddoNotOnChain, nil
	}
	return ddoOnChain, nil
}

// ddoPiecesFromHarmony reads a sector's piece list out of Curio's recorded
// metadata (the sectors_meta_pieces table, filled by curio guided-setup from the
// lotus-miner metadata datastore, and kept up to date for sectors Curio handles
// itself). piece_num is the piece's position in the sector, so ordering by it
// rebuilds the list in offset order, which is what PieceInSector accumulates.
//
// The bool reports whether the list was obtained. A missing table, a migration
// that has not run guided-setup yet, or a row that does not read is not a per-deal
// problem, so it answers false rather than failing: the caller counts those deals
// and reports them once, rather than turning a deployment ordering gap into a
// cutover failure. The error is returned for the log line, not to decide anything.
func ddoPiecesFromHarmony(ctx context.Context, hdb *harmonydb.DB, spID uint64, sectorNum abi.SectorNumber) ([]lapi.SectorPiece, bool, error) {
	var rows []ddoSectorPieceRow

	err := hdb.Select(ctx, &rows, `SELECT piece_num, piece_cid, piece_size FROM sectors_meta_pieces
		WHERE sp_id = $1 AND sector_num = $2 ORDER BY piece_num`, spID, sectorNum)
	if err != nil {
		return nil, false, err
	}

	pieces, err := ddoPiecesFromRows(sectorNum, rows)
	if err != nil {
		return nil, false, err
	}

	return pieces, true, nil
}

// ddoSectorPieceRow is one row of sectors_meta_pieces, the only recorded piece
// list a boost -> curio migration can read. piece_size is the padded size, and
// piece_num is the piece's position in the sector.
type ddoSectorPieceRow struct {
	PieceNum  int64  `db:"piece_num"`
	PieceCID  string `db:"piece_cid"`
	PieceSize int64  `db:"piece_size"`
}

// ddoPiecesFromRows converts recorded rows into the sealer's piece-list shape,
// in the order it was read (piece_num ascending), which is the offset order
// PieceInSector accumulates over. A row whose cid does not parse is returned as
// an error rather than skipped: it would shift every following piece's offset.
func ddoPiecesFromRows(sectorNum abi.SectorNumber, rows []ddoSectorPieceRow) ([]lapi.SectorPiece, error) {
	pieces := make([]lapi.SectorPiece, 0, len(rows))
	for _, r := range rows {
		pc, err := cid.Parse(r.PieceCID)
		if err != nil {
			return nil, fmt.Errorf("sector %d: piece %d: parsing piece cid %q: %w", sectorNum, r.PieceNum, r.PieceCID, err)
		}
		pieces = append(pieces, lapi.SectorPiece{
			Piece: abi.PieceInfo{Size: abi.PaddedPieceSize(r.PieceSize), PieceCID: pc},
		})
	}

	return pieces, nil
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

	// Counted, not fatal: these are the post-nv29 deals that could be neither
	// recovered nor refused because the sector's piece list could not be read.
	// See ddoPiecesFromHarmony.
	var noPieceSource int

	for i, deal := range deals {
		if i > 0 && i%100 == 0 {
			fmt.Printf("Migrating DDO Deals: %d / %d (%0.2f%%)\n", i, len(deals), float64(i)/float64(len(deals))*100)
		}
		llog := log.With("DDO Deal", deal.ID.String())
		// A fatal retry normally means Boost has already given the deal up for
		// good, so the migration leaves the row alone. The no-claim sentence is
		// the exception: past nv29 FIP-0118 writes no claim at all, so a
		// released Boost spent its ten minutes looking for one that can never
		// exist and wrote this on every post-nv29 deal it started -- exactly
		// the rows the migration exists to pick up. Excusing the sentence is not
		// the same as believing it: the deal still has to show evidence of
		// itself on chain below, and a sector carrying no weight is refused
		// there just as it would be for any other row.
		if fatalSkip(deal.Err, deal.Retry) {
			llog.Infow("Skipping as deal retry is fatal", "err", deal.Err)
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
		// From nv29 on FIP-0118 writes no claim, so a deal onboarded after the
		// upgrade has none by design and its sector is the evidence instead. The
		// sector and its piece list are read only when that can be needed, so the
		// pre-nv29 path keeps its single chain call.
		var (
			si          *miner.SectorOnChainInfo
			pieces      []lapi.SectorPiece
			piecesKnown bool
			pieceErr    error
		)
		if claim == nil {
			si, err = full.StateSectorGetInfo(ctx, maddr, deal.SectorID, ltypes.EmptyTSK)
			if err != nil {
				return fmt.Errorf("deal: %s: error getting the sector: %w", deal.ID.String(), err)
			}

			// A sector carrying no weight has no piece of ours to look for, so
			// the list is read only when the weight says there is data to explain.
			if sectorstatemgr.SectorCarriesData(si) {
				pieces, piecesKnown, pieceErr = ddoPiecesFromHarmony(ctx, hdb, mid, deal.SectorID)
			}
		}

		onChain, err := ddoDealOnChain(claim, si, pieces, piecesKnown, deal)
		if err != nil {
			return fmt.Errorf("deal: %s: %w", deal.ID.String(), err)
		}
		switch onChain {
		case ddoNoPieceSource:
			// The deal looks live (no claim, sector carries data) but this run
			// cannot see the sector's pieces, which is what the recovery is
			// decided on. Skipping is the safe half of the trade: migrating it
			// would be recovering a deal on the sector's weight alone, and that is
			// the case the piece check exists to refuse. The read error is not
			// raised to a failed run -- see ddoPiecesFromHarmony -- but it is
			// neither swallowed nor guessed at: pieceErr is nil when the list was
			// read and simply had nothing for this sector.
			noPieceSource++
			llog.Warnw("Skipping: no claim found and the sector's piece list gave no answer",
				"sector", deal.SectorID, "err", pieceErr)
			continue
		case ddoNotOnChain:
			llog.Infow("Skipping as neither a claim nor this deal's piece is in the sector", "sector", deal.SectorID)
			continue
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

		if deal.InboundFileSize == 0 {
			return fmt.Errorf("deal: %s: inbound file size is 0", deal.ID.String())
		}

		stx, err := mdb.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		_, err = hdb.BeginTransaction(ctx, func(tx *harmonydb.Tx) (bool, error) {
			if !a {
				// Add DDO deal to harmonyDB
				_, err = tx.Exec(`INSERT INTO market_direct_deals (uuid, sp_id, created_at, client, offline, verified,
                                  start_epoch, end_epoch, allocation_id, piece_cid, piece_size, raw_size, fast_retrieval, announce_to_ipni) 
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
				ON CONFLICT (uuid) DO NOTHING`,
					deal.ID.String(), mid, deal.CreatedAt, deal.Client.String(), true, true, deal.StartEpoch, deal.EndEpoch, deal.AllocationID,
					deal.PieceCID.String(), deal.PieceSize, deal.InboundFileSize, true, true)

				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to add the DDO deal to harmonyDB: %w", deal.ID.String(), err)
				}

				// Mark deal added to harmonyDB
				_, err = stx.Exec(`INSERT INTO Deals (ID, DB, LID) VALUES (?, TRUE, FALSE) ON CONFLICT(ID) DO NOTHING`, deal.ID.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark DDO deal migrated: %w", deal.ID.String(), err)
				}
			}

			if !b {
				// Add LID details to pieceDeal in HarmonyDB
				_, err = tx.Exec(`SELECT process_piece_deal($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
					deal.ID.String(), deal.PieceCID.String(), false, mid, deal.SectorID, deal.Offset, deal.PieceSize, deal.InboundFileSize, false, nil, false, 0)
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to update piece metadata and piece deal for DDO deal %s: %w", deal.ID.String(), deal.ID.String(), err)
				}

				// Mark deal added to pieceDeal in HarmonyDB
				_, err = stx.Exec(`UPDATE Deals SET LID = TRUE WHERE ID = ?`, deal.ID.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal LID migrated: %w", deal.ID.String(), err)
				}
			}

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

				// Mark deal added to pipeline in HarmonyDB
				_, err = stx.Exec(`UPDATE Deals SET Pipeline = TRUE WHERE ID = ?`, deal.ID.String())
				if err != nil {
					return false, fmt.Errorf("deal: %s: failed to mark deal Pipeline migrated: %w", deal.ID.String(), err)
				}
			}
			return true, nil
		}, harmonydb.OptionRetry())
		if err != nil {
			serr := stx.Rollback()
			if serr != nil {
				return fmt.Errorf("failed to commit Haromy changes: %w and failed to rollback transaction: %w", err, serr)
			}
			return fmt.Errorf("failed to commit Haromy changes: %w", err)
		}
		err = stx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit Haromy changes: %w", err)
		}
	}

	if noPieceSource > 0 {
		fmt.Printf("Skipped %d direct deal(s) with no claim whose sector's piece list could not be read (see the warnings above).\n"+
			"These are post-nv29 deals: the claim can never appear, so the sector's pieces are the only evidence left. "+
			"Run `curio guided-setup` (Migrate from existing Lotus-Miner) before this migration to populate "+
			"sectors_meta_pieces, then run this migration again -- these deals stay unmigrated until then.\n", noPieceSource)
	}

	return nil
}

func makeDB(cctx *cli.Context) (*harmonydb.DB, error) {
	dbConfig := harmonydb.Config{
		Username:    cctx.String("db-user"),
		Password:    cctx.String("db-password"),
		Hosts:       strings.Split(cctx.String("db-host"), ","),
		Database:    cctx.String("db-name"),
		Port:        cctx.String("db-port"),
		LoadBalance: cctx.Bool("db-load-balance"),
	}
	return harmonydb.NewFromConfig(dbConfig)
}
