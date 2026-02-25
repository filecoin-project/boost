package itests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/storagemarket"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/itests/kit"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestLIDCleanup(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// Setup datacap wallet and initialise a new ensemble with datacap keys
	rootKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	verifier1Key, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	bal, err := types.ParseFIL("10000fil")
	require.NoError(t, err)

	var eopts []kit.EnsembleOpt
	eopts = append(eopts, kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.Account(verifier1Key, abi.NewTokenAmount(bal.Int64())))
	esemble := kit.NewEnsemble(t, eopts...)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithEnsemble(esemble))
	opts = append(opts, framework.SetProvisionalWalletBalances(int64(9e18)))
	opts = append(opts, framework.WithStartEpochSealingBuffer(30))
	opts = append(opts, framework.WithMaxStagingDealsBytes(100000000))
	f := framework.NewTestFramework(ctx, t, opts...)
	esemble.Start()
	blockTime := 100 * time.Millisecond
	esemble.BeginMining(blockTime)

	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Send funds to PSD wallet as it is being used for POST
	info, err := f.FullNode.StateMinerInfo(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)
	addresses := []address.Address{info.Owner, info.Worker}
	addresses = append(addresses, info.ControlAddresses...)
	eg := errgroup.Group{}
	eg.SetLimit(4)
	for i := 0; i < 6; i++ {
		for _, a := range addresses {
			addr := a
			eg.Go(func() error {
				return framework.SendFunds(ctx, f.FullNode, addr, abi.NewTokenAmount(int64(9e18)))
			})
		}
	}
	err = eg.Wait()
	require.NoError(t, err)

	// Give the boost client's address enough datacap to make the deal
	err = f.AddClientDataCap(t, ctx, rootKey, verifier1Key)
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 7048576
	randomFilepath1, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)

	randomFilepath2, err := testutil.CreateRandomFile(tempdir, 6, fileSize)
	require.NoError(t, err)

	// NOTE: these calls to CreateDenseCARv2 have the identity CID builder enabled so will
	// produce a root identity CID for this case. So we're testing deal-making and retrieval
	// where a DAG has an identity CID root
	rootCid1, carFilepath1, err := testutil.CreateDenseCARv2(tempdir, randomFilepath1)
	require.NoError(t, err)

	rootCid2, carFilepath2, err := testutil.CreateDenseCARv2(tempdir, randomFilepath2)
	require.NoError(t, err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid1 := uuid.New()

	// Make a deal
	res1, err := f.MakeDummyDeal(dealUuid1, carFilepath1, rootCid1, server.URL+"/"+filepath.Base(carFilepath1), true)
	require.NoError(t, err)
	require.True(t, res1.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res1))
	res11, err := f.Boost.BoostOfflineDealWithData(context.Background(), dealUuid1, carFilepath1, false)
	require.NoError(t, err)
	require.True(t, res11.Accepted)

	dealUuid2 := uuid.New()
	res2, err := f.MakeDummyDeal(dealUuid2, carFilepath2, rootCid2, server.URL+"/"+filepath.Base(carFilepath2), true)
	require.NoError(t, err)
	require.True(t, res2.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res2))
	res21, err := f.Boost.BoostOfflineDealWithData(context.Background(), dealUuid2, carFilepath2, false)
	require.NoError(t, err)
	require.True(t, res21.Accepted)

	// Create a CAR file for DDO deal
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)
	_, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)
	commp, err := storagemarket.GenerateCommPLocally(carFilepath)
	require.NoError(t, err)
	t.Logf("Piece CID: %s, Piece Size: %d", commp.PieceCID.String(), commp.Size)

	mid, err := address.IDFromAddress(f.MinerAddr)
	require.NoError(t, err)
	var pieceInfos []util.PieceInfos

	pieceInfos = append(pieceInfos, util.PieceInfos{
		Cid:       commp.PieceCID,
		Size:      int64(commp.Size),
		Miner:     abi.ActorID(mid),
		MinerAddr: f.MinerAddr,
		Tmin:      verifregst.MinimumVerifiedAllocationTerm,
		Tmax:      verifregst.MaximumVerifiedAllocationTerm,
		Exp:       verifregst.MaximumVerifiedAllocationExpiration,
	})

	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, f.ClientAddr, 10)
	require.NoError(t, err)

	sm, err := f.FullNode.MpoolPushMessage(ctx, allocateMsg[0], nil)
	require.NoError(t, err)

	_, err = f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, 1e10, true)
	require.NoError(t, err)

	allocations, err := f.FullNode.StateGetAllocations(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Len(t, allocations, 1)

	var allocationId uint64
	for id := range allocations {
		allocationId = uint64(id)
	}

	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)

	startEpoch := head.Height() + 1000
	endEpoch := head.Height() + +2880*400

	dealUuid := uuid.New()
	ddParams := smtypes.DirectDealParams{
		DealUUID:           dealUuid,
		AllocationID:       verifreg.AllocationId(allocationId),
		PieceCid:           commp.PieceCID,
		ClientAddr:         f.ClientAddr,
		StartEpoch:         startEpoch,
		EndEpoch:           endEpoch,
		FilePath:           carFilepath,
		DeleteAfterImport:  false,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	rej, err := f.Boost.BoostDirectDeal(ctx, ddParams)
	require.NoError(t, err)
	if rej != nil && rej.Reason != "" {
		require.Fail(t, "direct data import rejected: %s", rej.Reason)
	}
	t.Log("Direct data import scheduled for execution")

	// Wait for sector to start sealing
	time.Sleep(2 * time.Second)

	// Wait for all 5 sectors to get to proving state
	states := []lapi.SectorState{lapi.SectorState(sealing.Proving)}
	require.Eventuallyf(t, func() bool {
		stateList, err := f.LotusMiner.SectorsListInStates(ctx, states)
		require.NoError(t, err)
		return len(stateList) == 5
	}, 10*time.Minute, 2*time.Second, "sectors are still not proving after 5 minutes")

	// Verify that LID has entries for all deals
	prop1, err := cborutil.AsIpld(&res1.DealParams.ClientDealProposal)
	require.NoError(t, err)

	prop2, err := cborutil.AsIpld(&res2.DealParams.ClientDealProposal)
	require.NoError(t, err)

	mhs, err := f.Boost.BoostIndexerListMultihashes(ctx, prop1.Cid().Bytes())
	require.NoError(t, err)
	require.Greater(t, len(mhs), 0)

	mhs, err = f.Boost.BoostIndexerListMultihashes(ctx, prop2.Cid().Bytes())
	require.NoError(t, err)
	require.Greater(t, len(mhs), 0)

	ddo, err := dealUuid.MarshalBinary()
	require.NoError(t, err)

	mhs, err = f.Boost.BoostIndexerListMultihashes(ctx, ddo)
	require.NoError(t, err)
	require.Greater(t, len(mhs), 0)

	// Wait for wdPost
	require.Eventuallyf(t, func() bool {
		mActor, err := f.FullNode.StateGetActor(ctx, f.MinerAddr, types.EmptyTSK)
		require.NoError(t, err)
		store := adt.WrapStore(ctx, cbor.NewCborStore(blockstore.NewAPIBlockstore(f.FullNode)))
		mas, err := miner.Load(store, mActor)
		require.NoError(t, err)
		unproven, err := miner.AllPartSectors(mas, miner.Partition.UnprovenSectors)
		require.NoError(t, err)
		count, err := unproven.Count()
		require.NoError(t, err)
		return count == 0
	}, blockTime*(2880), 3*time.Second, "timeout waiting for wdPost")

	// Terminate DDO sector and a deal sector
	err = f.LotusMiner.SectorTerminate(ctx, abi.SectorNumber(2))
	require.NoError(t, err)
	err = f.LotusMiner.SectorTerminate(ctx, abi.SectorNumber(4))
	require.NoError(t, err)
	time.Sleep(2 * time.Second)
	_, err = f.LotusMiner.SectorTerminateFlush(ctx)
	require.NoError(t, err)

	// Keep trying to terminate in case of deadline issues
	require.Eventually(t, func() bool {
		tpending, err := f.LotusMiner.SectorTerminatePending(ctx)
		require.NoError(t, err)
		if len(tpending) == 0 {
			return true
		}
		_, err = f.LotusMiner.SectorTerminateFlush(ctx)
		require.NoError(t, err)
		return false
	}, blockTime*(60*2), 1*time.Second, "timeout waiting for sectors to terminate")

	// Wait for terminate message to be processed
	states = []lapi.SectorState{lapi.SectorState(sealing.TerminateFinality)}
	require.Eventuallyf(t, func() bool {
		stateList, err := f.LotusMiner.SectorsListInStates(ctx, states)
		require.NoError(t, err)
		return len(stateList) == 2
	}, time.Second*15, 1*time.Second, "timeout waiting for sectors to reach TerminateFinality")

	var bigger abi.ChainEpoch

	bigger = res2.DealParams.ClientDealProposal.Proposal.StartEpoch

	if res1.DealParams.ClientDealProposal.Proposal.StartEpoch > res2.DealParams.ClientDealProposal.Proposal.StartEpoch {
		bigger = res1.DealParams.ClientDealProposal.Proposal.StartEpoch
	}

	if bigger < ddParams.StartEpoch {
		bigger = ddParams.StartEpoch
	}

	// Wait till all deal start epochs have passed
	require.Eventuallyf(t, func() bool {
		h, err := f.FullNode.ChainHead(ctx)
		require.NoError(t, err)
		return h.Height() > bigger
	}, time.Minute*5, time.Second, "timeout waiting for start epochs")

	// Clean up LID
	err = f.Boost.PdCleanup(ctx)
	require.NoError(t, err)

	// Listing multihashes for DDO deal should fail
	_, err = f.Boost.BoostIndexerListMultihashes(ctx, ddo)
	require.ErrorContains(t, err, " key not found")

	st, err := f.LotusMiner.SectorsStatus(ctx, abi.SectorNumber(3), true)
	require.NoError(t, err)
	require.Len(t, st.Pieces, 1)
	var removedProp cid.Cid
	var remainingProp cid.Cid

	if res1.DealParams.ClientDealProposal.Proposal.PieceCID.Equals(st.Pieces[0].Piece.PieceCID) {
		removedProp = prop2.Cid()
		remainingProp = prop1.Cid()
	} else {
		removedProp = prop1.Cid()
		remainingProp = prop2.Cid()
	}

	// Listing multihashes for removed Boost deal should fail
	_, err = f.Boost.BoostIndexerListMultihashes(ctx, removedProp.Bytes())
	require.ErrorContains(t, err, " key not found")

	// Listing multihashes for remaining deal should succeed
	mhs, err = f.Boost.BoostIndexerListMultihashes(ctx, remainingProp.Bytes())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(mhs), 1)
}
