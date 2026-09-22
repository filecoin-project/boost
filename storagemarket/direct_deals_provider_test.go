package storagemarket

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
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

// sealedSectorPipeline reports a sector that has finished sealing. Only the one
// method watchSealingUpdates calls is implemented; the embedded interface panics
// on anything else, so the test cannot quietly start depending on more.
type sealedSectorPipeline struct {
	sealingpipeline.API
	state lapi.SectorState
}

func (s *sealedSectorPipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	return lapi.SectorInfo{State: s.state}, nil
}

// claimLookupNode answers the chain queries watchSealingUpdates makes. Its chain
// is always at nv29, as it is when the question is really asked, so a check
// reading the current version instead of the sector's activation gets a wrong
// answer rather than accidentally a right one.
type claimLookupNode struct {
	v1api.FullNode
	nv29Height      abi.ChainEpoch
	noUpgradeHeight bool
	activation      abi.ChainEpoch
	sectorErr       error
	sectorMissing   bool
	claim           *verifreg9types.Claim
}

func (c *claimLookupNode) StateNetworkVersion(context.Context, ltypes.TipSetKey) (network.Version, error) {
	return network.Version29, nil
}

func (c *claimLookupNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	return c.claim, nil
}

func (c *claimLookupNode) StateSectorGetInfo(context.Context, address.Address, abi.SectorNumber, ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	if c.sectorMissing {
		return nil, nil
	}
	return &miner.SectorOnChainInfo{Activation: c.activation}, nil
}

func (c *claimLookupNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	if c.noUpgradeHeight {
		// A node built before nv29 has no such field: it decodes to a zero epoch.
		return &lapi.NetworkParams{}, nil
	}
	return &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeXxHeight: c.nv29Height},
	}, nil
}

// nv29Height is the upgrade epoch these tests date sectors against.
const nv29Height = abi.ChainEpoch(1000)

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

// newWatchSealingHarness wires a provider whose deal has a sector in a final
// sealing state, so watchSealingUpdates goes straight to the claim question.
func newWatchSealingHarness(t *testing.T, node *claimLookupNode) (*DirectDealsProvider, *types.DirectDeal) {
	t.Helper()

	node.nv29Height = nv29Height

	_, dealLogger := newTestStores(t)

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	ddp := &DirectDealsProvider{
		ctx:         context.Background(),
		Address:     maddr,
		fullnodeApi: node,
		sps:         &sealedSectorPipeline{state: lapi.SectorState(sealing.Proving)},
		dealLogger:  dealLogger,
	}

	entry := &types.DirectDeal{
		ID:           uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		SectorID:     abi.SectorNumber(2),
	}
	return ddp, entry
}

// TestWatchSealingUpdatesSealedAfterNv29 covers a sector activated after the
// upgrade, where the sealed sector is the whole of the outcome and reporting the
// missing claim as a failure marks a deal that is fine as broken.
func TestWatchSealingUpdatesSealedAfterNv29(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height + 1})

	require.Nil(t, ddp.watchSealingUpdates(entry),
		"a deal whose sector sealed after nv29 is complete even though no claim was written")
}

// TestWatchSealingUpdatesClaimFound pins the pre-nv29 path: a claim that matches
// the sector still completes the deal, so the nv29 branch has not displaced the
// normal case.
func TestWatchSealingUpdatesClaimFound(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height - 1, claim: claim})

	require.Nil(t, ddp.watchSealingUpdates(entry))
}

// TestWatchSealingUpdatesClaimSectorMismatch checks that a claim on another
// sector stays fatal whatever the sector's activation, so the new branch does not
// swallow a real inconsistency along with the expected absences.
func TestWatchSealingUpdatesClaimSectorMismatch(t *testing.T) {
	for name, node := range map[string]*claimLookupNode{
		"sealed before nv29": {activation: nv29Height - 1, claim: &verifreg9types.Claim{Sector: abi.SectorNumber(3)}},
		"sealed after nv29":  {activation: nv29Height + 1, claim: &verifreg9types.Claim{Sector: abi.SectorNumber(3)}},
	} {
		t.Run(name, func(t *testing.T) {
			ddp, entry := newWatchSealingHarness(t, node)

			derr := ddp.watchSealingUpdates(entry)
			require.NotNil(t, derr)
			require.Equal(t, types.DealRetryFatal, derr.retry)
			require.Contains(t, derr.Error(), "sector mismatch")
		})
	}
}

// TestWatchSealingUpdatesClaimFoundAtNv29 covers a deal that got its claim
// before the upgrade and is re-checked after: it is still on chain, so use it.
func TestWatchSealingUpdatesClaimFoundAtNv29(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height + 1, claim: claim})

	require.Nil(t, ddp.watchSealingUpdates(entry))
}

// TestResolveClaimNoClaimSealedBeforeNv29KeepsWaiting pins what dating by the
// sector buys: this sector was activated while claims were still written, so the
// missing one is a real fault and the deal must not be settled.
func TestResolveClaimNoClaimSealedBeforeNv29KeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height - 1})

	done, err := ddp.resolveClaim(entry)
	require.Nil(t, err)
	require.False(t, done, "a pre-nv29 sector's missing claim is a fault, not the expected nv29 absence")
}

// TestResolveClaimSectorNotOnChainKeepsWaiting covers a sector that is gone, so
// there is no activation to date it by: it must keep looking, not complete.
func TestResolveClaimSectorNotOnChainKeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{sectorMissing: true})

	done, err := ddp.resolveClaim(entry)
	require.Nil(t, err)
	require.False(t, done)
}

// TestResolveClaimLookupFailureRetries checks that a chain query that fails is
// not mistaken for either answer: the deal is left open and the caller retries.
func TestResolveClaimLookupFailureRetries(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{sectorErr: context.DeadlineExceeded})

	done, err := ddp.resolveClaim(entry)
	require.NotNil(t, err)
	require.False(t, done)
	require.Equal(t, types.DealRetryAuto, err.retry)
}

// TestResolveClaimUpgradeHeightNotReportedRetries covers a node too old to say
// when nv29 activates. The zero a missing field decodes to would put every sector
// past the upgrade, so this sector's activation alone must not complete the deal.
func TestResolveClaimUpgradeHeightNotReportedRetries(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height + 1, noUpgradeHeight: true})

	done, err := ddp.resolveClaim(entry)
	require.NotNil(t, err, "an unreadable upgrade height must not be read as 'after the upgrade'")
	require.False(t, done)
	require.Equal(t, types.DealRetryAuto, err.retry)
	require.Contains(t, err.Error(), "nv29 upgrade height")
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

	mh, err := multihash.Sum([]byte("test piece"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

// paramsNode serves only the network params query Nv29UpgradeHeight makes.
type paramsNode struct {
	v1api.FullNode
	params *lapi.NetworkParams
	err    error
}

func (p *paramsNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	return p.params, p.err
}

func paramsAt(height abi.ChainEpoch) *paramsNode {
	return &paramsNode{params: &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeXxHeight: height},
	}}
}

// TestNv29UpgradeHeight covers the values a full node can report, in particular
// the ones a caller must not compare a sector's activation against.
func TestNv29UpgradeHeight(t *testing.T) {
	tests := map[string]struct {
		node      *paramsNode
		want      abi.ChainEpoch
		expectErr bool
	}{
		"a scheduled upgrade": {
			node: paramsAt(4_000_000),
			want: 4_000_000,
		},
		"unscheduled": {
			// Lotus parks an upgrade with no epoch yet far in the future.
			node: paramsAt(999999999999999),
			want: 999999999999999,
		},
		"the field is missing from the response": {
			node:      &paramsNode{params: &lapi.NetworkParams{}},
			expectErr: true,
		},
		"negative": {
			// Test networks spell "active from genesis" as a negative epoch.
			node:      paramsAt(-24),
			expectErr: true,
		},
		"the query fails": {
			node:      &paramsNode{err: context.DeadlineExceeded},
			expectErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			height, err := Nv29UpgradeHeight(context.Background(), tc.node)
			if tc.expectErr {
				require.Error(t, err)
				require.Zero(t, height, "a rejected height must not leak out as a usable epoch")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, height)
		})
	}
}
