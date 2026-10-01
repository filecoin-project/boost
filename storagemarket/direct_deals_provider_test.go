package storagemarket

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

// fixedSectorPipeline reports one sector record, whatever it is asked.
type fixedSectorPipeline struct {
	sealingpipeline.API
	info lapi.SectorInfo
}

func (s *fixedSectorPipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	return s.info, nil
}

// sealingSequencePipeline walks the sealer through a list of states, holding on the last.
type sealingSequencePipeline struct {
	sealingpipeline.API
	states []lapi.SectorState
	calls  int
}

func (s *sealingSequencePipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	i := s.calls
	if i >= len(s.states) {
		i = len(s.states) - 1
	}
	s.calls++
	return lapi.SectorInfo{State: s.states[i]}, nil
}

// preNv29Sector: the 10x came from the deal's own verified weight, flag clear, a claim.
func preNv29Sector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:       sector,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.NewInt(1 << 20),
	}
}

// nv29Sector carries piece data and the FULL_QA_POWER bit, with no claim written for it.
func nv29Sector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	si := preNv29Sector(sector)
	si.Flags = miner.FULL_QA_POWER
	return si
}

func emptySector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:       sector,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		Flags:              miner.FULL_QA_POWER,
	}
}

// tenXByDatacapSector is pre-nv29 with its whole weight from datacap, so lotus's
// miner.SectorIsFullQaPower reads it as full QA power where the flag says otherwise.
func tenXByDatacapSector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	const (
		tenXDuration abi.ChainEpoch = 1 << 20
		thirtyTwoGiB abi.SectorSize = 32 << 30
	)

	si := preNv29Sector(sector)
	si.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
	si.Expiration = tenXDuration
	si.VerifiedDealWeight = big.Mul(big.NewInt(int64(thirtyTwoGiB)), big.NewInt(int64(tenXDuration)))
	return si
}

// chainNode answers the chain reads the direct deal paths make; anything else panics.
type chainNode struct {
	v1api.FullNode

	sector    *miner.SectorOnChainInfo
	sectorErr error
	claim     *verifreg9types.Claim
	claimErr  error

	sectorReads int
	claimReads  int
}

func (c *chainNode) StateSectorGetInfo(_ context.Context, _ address.Address, sector abi.SectorNumber, _ ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	c.sectorReads++
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	return c.sector, nil
}

func (c *chainNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	c.claimReads++
	return c.claim, c.claimErr
}

// newTestStores creates the sqlite database a DirectDealsProvider needs, with
// the boost tables in it.
func newTestStores(t *testing.T) (*db.DirectDealsDB, *logs.DealLogger) {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	sqldb, err := db.SqlDB(f.Name())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqldb.Close() })
	require.NoError(t, db.CreateAllBoostTables(context.Background(), sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	return db.NewDirectDealsDB(sqldb), logs.NewDealLogger(db.NewLogsDB(sqldb))
}

// newWatchSealingHarness builds a provider watching one deal, sealer parked, node answering.
func newWatchSealingHarness(t *testing.T, node *chainNode, state lapi.SectorState) (*DirectDealsProvider, *types.DirectDeal) {
	t.Helper()

	_, dealLogger := newTestStores(t)

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	ddp := &DirectDealsProvider{
		ctx:         context.Background(),
		Address:     maddr,
		fullnodeApi: node,
		sps:         &fixedSectorPipeline{info: lapi.SectorInfo{State: state}},
		dealLogger:  dealLogger,
	}

	entry := &types.DirectDeal{
		ID:           uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		SectorID:     abi.SectorNumber(2),
		PieceCID:     testPieceCid(t),
	}
	return ddp, entry
}

func TestWatchSealingUpdatesSealedAfterNv29(t *testing.T) {
	node := &chainNode{sector: nv29Sector(abi.SectorNumber(2))}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	require.Nil(t, ddp.watchSealingUpdates(entry),
		"a deal whose sector onboarded after nv29 is complete even though no claim was written")
	require.Zero(t, node.claimReads, "the flag answers for this sector; the claim is not read")
}

// TestWatchSealingUpdatesClaimFound pins the pre-nv29 path: a claim that matches
// the sector still completes the deal, so the nv29 branch has not displaced the
// normal case.
func TestWatchSealingUpdatesClaimFound(t *testing.T) {
	node := &chainNode{
		sector: preNv29Sector(abi.SectorNumber(2)),
		claim:  &verifreg9types.Claim{Sector: abi.SectorNumber(2)},
	}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	require.Nil(t, ddp.watchSealingUpdates(entry))
}

func TestWatchSealingUpdatesNoClaimOnASectorThisNodeHasFails(t *testing.T) {
	node := &chainNode{sector: preNv29Sector(abi.SectorNumber(2))}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	derr := ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryFatal, derr.retry)
	require.ErrorIs(t, derr.error, ErrNoClaimFound)
}

func TestWatchSealingUpdatesClaimSectorMismatch(t *testing.T) {
	node := &chainNode{
		sector: preNv29Sector(abi.SectorNumber(2)),
		claim:  &verifreg9types.Claim{Sector: abi.SectorNumber(3)},
	}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	derr := ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryFatal, derr.retry)
	require.Contains(t, derr.Error(), "sector mismatch")
}

func TestWatchSealingUpdatesSectorNotOnThisNodesChain(t *testing.T) {
	node := &chainNode{}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	derr := ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryAuto, derr.retry)
	require.NotErrorIs(t, derr.error, ErrNoClaimFound,
		"a local lag must not be able to produce the migration's verdict")
}

func TestWatchSealingUpdatesChainReadFailureRetries(t *testing.T) {
	node := &chainNode{sectorErr: context.DeadlineExceeded}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Proving))

	derr := ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryAuto, derr.retry)
	require.ErrorIs(t, derr.error, context.DeadlineExceeded)
}

func TestWatchSealingUpdatesSealingFailed(t *testing.T) {
	for _, state := range []sealing.SectorState{
		sealing.FailedUnrecoverable,
		sealing.Removed,
		sealing.Terminating,
	} {
		t.Run(string(state), func(t *testing.T) {
			node := &chainNode{sector: emptySector(abi.SectorNumber(2))}
			ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(state))

			derr := ddp.watchSealingUpdates(entry)
			require.NotNil(t, derr)
			require.Equal(t, types.DealRetryFatal, derr.retry)
			require.ErrorIs(t, derr.error, ErrSectorSealingFailed)
			require.NotErrorIs(t, derr.error, ErrNoClaimFound)
			require.Zero(t, node.sectorReads,
				"a failed sector is the sealer's own verdict, so it is not put to the chain")
		})
	}
}

func TestWatchSealingUpdatesWaitsForTheSealer(t *testing.T) {
	node := &chainNode{sector: nv29Sector(abi.SectorNumber(2))}
	ddp, entry := newWatchSealingHarness(t, node, lapi.SectorState(sealing.Packing))
	ddp.sps = &sealingSequencePipeline{
		states: []lapi.SectorState{lapi.SectorState(sealing.Packing), lapi.SectorState(sealing.Proving)},
	}
	ddp.sealingPollEvery = time.Millisecond

	require.Nil(t, ddp.watchSealingUpdates(entry))
	require.Equal(t, 2, ddp.sps.(*sealingSequencePipeline).calls)
	require.Equal(t, 1, node.sectorReads,
		"the chain is read once, when the sealer is done, not on every poll")
}

// TestErrNoClaimFoundWording pins the sentence migrate-curio matches on: a deal
// carries its error as text alone, so changing the wording would strand data.
func TestErrNoClaimFoundWording(t *testing.T) {
	require.Equal(t, "no claim was found for a piece onboarded before nv29", ErrNoClaimFound.Error())
	require.True(t, errors.Is(ErrNoClaimFound, ErrNoClaimFound))
}

// TestNoClaimFoundIsRecordedVerbatim: dealMakingError keeps no error chain, so the
// sentinel must reach the deal record undecorated for migrate-curio to match it.
func TestNoClaimFoundIsRecordedVerbatim(t *testing.T) {
	derr := &dealMakingError{retry: types.DealRetryFatal, error: ErrNoClaimFound}

	require.Equal(t, ErrNoClaimFound.Error(), derr.Error())
}

// vanishingAllocationNode serves the node queries Import makes, with an
// allocation that is there for Accept's lookup and gone for the one Import runs
// straight after it - the window in which another deal can claim it. Only the
// methods on this path are implemented, so the embedded interface panics if the
// test ever starts depending on more.
type vanishingAllocationNode struct {
	v1api.FullNode
	nv          network.Version
	miner       address.Address
	allocation  *verifreg9types.Allocation
	lookupCalls int
}

func (n *vanishingAllocationNode) ChainHead(context.Context) (*ltypes.TipSet, error) {
	return mockTipset(n.miner, 100)
}

func (n *vanishingAllocationNode) StateNetworkVersion(context.Context, ltypes.TipSetKey) (network.Version, error) {
	return n.nv, nil
}

func (n *vanishingAllocationNode) StateGetAllocation(context.Context, address.Address, verifreg9types.AllocationId, ltypes.TipSetKey) (*verifreg9types.Allocation, error) {
	n.lookupCalls++
	if n.lookupCalls > 1 {
		return nil, nil
	}
	return n.allocation, nil
}

// TestImportRejectsAllocationThatDisappears covers the allocation going away
// between Accept, which checks it is there, and the lookup Import does after it.
// StateGetAllocation reports a missing allocation as (nil, nil), so Import used
// to dereference it and panic on a perm:admin API call.
func TestImportRejectsAllocationThatDisappears(t *testing.T) {
	ctx := context.Background()

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)

	clientAddr, err := address.NewIDAddress(1001)
	require.NoError(t, err)

	node := &vanishingAllocationNode{
		nv:         network.Version28,
		miner:      maddr,
		allocation: &verifreg9types.Allocation{Provider: abi.ActorID(mid), TermMin: abi.ChainEpoch(2880)},
	}

	directDealsDB, dealLogger := newTestStores(t)
	ddp := &DirectDealsProvider{
		ctx:           ctx,
		Address:       maddr,
		fullnodeApi:   node,
		directDealsDB: directDealsDB,
		dealLogger:    dealLogger,
	}

	res, err := ddp.Import(ctx, types.DirectDealParams{
		DealUUID:     uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		PieceCid:     testPieceCid(t),
		ClientAddr:   clientAddr,
		StartEpoch:   200,
		EndEpoch:     300,
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.False(t, res.Accepted, "the deal must be turned away, not panic")
	require.Contains(t, res.Reason, "not found")
	require.Equal(t, 2, node.lookupCalls, "Accept should have found the allocation on the first lookup")

	// Nothing should have been queued for a deal that was rejected.
	deals, err := directDealsDB.ListActive(ctx)
	require.NoError(t, err)
	require.Empty(t, deals)
}

// TestAcceptRejectsDirectDealAtNv29 pins the provider's side of the nv29
// rejection to the shared reason. `boostd import-direct` refuses the same deal
// up front with that string, so this is what keeps the two answers identical
// instead of two sentences that drift apart.
//
// The deal is otherwise well formed and has an allocation, so the network
// version is the only thing that can turn it away.
func TestAcceptRejectsDirectDealAtNv29(t *testing.T) {
	ctx := context.Background()

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)

	clientAddr, err := address.NewIDAddress(1001)
	require.NoError(t, err)

	node := &vanishingAllocationNode{
		nv:         network.Version29,
		miner:      maddr,
		allocation: &verifreg9types.Allocation{Provider: abi.ActorID(mid), TermMin: abi.ChainEpoch(2880)},
	}

	directDealsDB, dealLogger := newTestStores(t)
	ddp := &DirectDealsProvider{
		ctx:           ctx,
		Address:       maddr,
		fullnodeApi:   node,
		directDealsDB: directDealsDB,
		dealLogger:    dealLogger,
	}

	res, err := ddp.Import(ctx, types.DirectDealParams{
		DealUUID:     uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		PieceCid:     testPieceCid(t),
		ClientAddr:   clientAddr,
		StartEpoch:   200,
		EndEpoch:     300,
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.False(t, res.Accepted)
	require.Equal(t, DirectDealRejectionAtNv29, res.Reason)

	// The version is checked before the allocation is looked up, so a deal that
	// cannot be served at all does not even reach the chain queries.
	require.Zero(t, node.lookupCalls, "the network version should be settled before anything else is asked")
}

func testPieceCid(t *testing.T) cid.Cid {
	t.Helper()
	return testPieceCidSeed(t, "test piece")
}

func testPieceCidSeed(t *testing.T, seed string) cid.Cid {
	t.Helper()

	mh, err := multihash.Sum([]byte(seed), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

func TestSectorOnboarded(t *testing.T) {
	for name, tc := range map[string]struct {
		sector *miner.SectorOnChainInfo
		want   bool
	}{
		"a sector onboarded before nv29": {
			// What fails if the dating is read off miner.SectorIsFullQaPower instead of the bit.
			sector: tenXByDatacapSector(abi.SectorNumber(2)),
			want:   false,
		},
		"a sector onboarded from nv29 on": {
			sector: nv29Sector(abi.SectorNumber(2)),
			want:   true,
		},
		"a flagged sector holding no piece": {
			sector: emptySector(abi.SectorNumber(2)),
			want:   false,
		},
		"nothing on chain for the sector": {
			sector: nil,
			want:   false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, SectorOnboarded(tc.sector))
		})
	}
}
