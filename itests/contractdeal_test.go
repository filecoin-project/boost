package itests

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	builtintypes "github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v10/eam"
	"github.com/filecoin-project/go-state-types/exitcode"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
)

// TestContractDeal deploys a contract, triggers a msg on it (deal proposal event) and
// verifies that we handle this message on Boost side
func TestContractDeal(t *testing.T) {
	require := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	f := framework.NewTestFramework(ctx, t)
	err := f.Start()
	require.NoError(err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 2000000
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(err)

	// NOTE: these calls to CreateDenseCARv2 have the identity CID builder enabled so will
	// produce a root identity CID for this case. So we're testing deal-making and retrieval
	// where a DAG has an identity CID root
	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(err)
	defer server.Close()

	// Create a new dummy deal
	//log.Debug("creating dummy deal")
	//dealUuid := uuid.New()

	_ = rootCid
	_ = carFilepath

	// Make a deal
	//res, err := f.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false)
	//require.NoError(t, err)
	//require.True(t, res.Result.Accepted)
	//log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	//time.Sleep(2 * time.Second)

	// install contract
	contract, err := hex.DecodeString(contractHex)
	require.NoError(err)

	fromAddr, err := f.FullNode.WalletDefaultAddress(ctx)
	require.NoError(err)

	// We hash the f1/f3 address into the EVM's address space when deploying contracts from
	// accounts.
	effectiveEvmAddress := effectiveEthAddressForCreate(t, fromAddr)
	ethAddr := f.FullNode.EVM().ComputeContractAddress(effectiveEvmAddress, 1)

	contractFilAddr, err := ethAddr.ToFilecoinAddress()
	require.NoError(err)

	//transfer half the wallet balance
	bal, err := f.FullNode.WalletBalance(ctx, f.FullNode.DefaultKey.Address)
	require.NoError(err)
	sendAmount := big.Div(bal, big.NewInt(2))
	f.FullNode.EVM().TransferValueOrFail(ctx, fromAddr, contractFilAddr, sendAmount)

	// Check if actor at new address is a placeholder actor
	actor, err := f.FullNode.StateGetActor(ctx, contractFilAddr, types.EmptyTSK)
	require.NoError(err)
	require.True(builtin.IsPlaceholderActor(actor.Code))

	// Create and deploy evm actor
	result := createAndDeploy(ctx, t, f.FullNode, fromAddr, contract)

	// Check if eth address returned from CreateExternal is the same as eth address predicted at the start
	createdEthAddr := getEthAddressTX(ctx, t, f.FullNode, result, ethAddr)
	//require.Equal(t, ethAddr, createdEthAddr)
	spew.Dump(createdEthAddr)

	//result := f.FullNode.EVM().DeployContract(ctx, fromAddr, contract)

	//idAddr, err := address.NewIDAddress(result.Receipt.ActorID)
	//require.NoError(err)
	//t.Logf("actor ID address is %s", idAddr)
	t.Logf("actor ID address is %s", createdEthAddr)

	var (
		earliest = "earliest"
		latest   = "latest"
	)

	// Install a filter.
	//filter, err := f.FullNode.EthNewFilter(ctx, &ethtypes.EthFilterSpec{
	//FromBlock: &earliest,
	//ToBlock:   &latest,
	//})
	//require.NoError(err)

	// No logs yet.
	//res, err := f.FullNode.EthGetFilterLogs(ctx, filter)
	//require.NoError(err)
	//require.Empty(res.Results)

	idAddr, err := createdEthAddr.ToFilecoinAddress()
	require.NoError(err)

	spew.Dump(idAddr)

	//ret, err := f.FullNode.EVM().InvokeSolidity(ctx, f.FullNode.DefaultKey.Address, idAddr, []byte(`5902c4c1`), []byte(`117182000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000001800000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001e0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000016800000000000000000000000000000000000000000000000000000000000080ac00000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000260000000000000000000000000000000000000000000000000000000000000028000000000000000000000000000000000000000000000000000000000000000260155a0e40220b6994f9351f9ae08e09bf5fdbbd1bd50f29bbc53ce3ef8c0bb82f10e71d48c2700000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003e6261666b32627a61636563336a737434746b6834323463686174703237336f367278766970666735346b70686435366761786f627063647472327367636f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000`))
	ret, err := f.FullNode.EVM().InvokeSolidity(ctx, f.FullNode.DefaultKey.Address, idAddr, parameters[:4], parameters[4:])
	require.NoError(err)
	spew.Dump(ret)

	// log a zero topic event with data
	//ret, err := f.FullNode.EVM().InvokeSolidity(ctx, fromAddr, idAddr, []byte{0x00, 0x00, 0x00, 0x00}, nil)
	//require.NoError(err)
	//require.True(ret.Receipt.ExitCode.IsSuccess(), "contract execution failed")
	//require.NotNil(ret.Receipt.EventsRoot)
	//fmt.Println(ret)
	//fmt.Printf("Events:\n %+v\n", f.FullNode.EVM().LoadEvents(ctx, *ret.Receipt.EventsRoot))

	//// log a zero topic event with no data
	//ret, err = f.FullNode.EVM().InvokeSolidity(ctx, fromAddr, idAddr, []byte{0x00, 0x00, 0x00, 0x01}, nil)
	//require.NoError(err)
	//require.True(ret.Receipt.ExitCode.IsSuccess(), "contract execution failed")
	//fmt.Println(ret)
	//fmt.Printf("Events:\n %+v\n", f.FullNode.EVM().LoadEvents(ctx, *ret.Receipt.EventsRoot))

	//// log a four topic event with data
	//ret, err = f.FullNode.EVM().InvokeSolidity(ctx, fromAddr, idAddr, []byte{0x00, 0x00, 0x00, 0x02}, nil)
	//require.NoError(err)
	//require.True(ret.Receipt.ExitCode.IsSuccess(), "contract execution failed")
	//fmt.Println(ret)
	//fmt.Printf("Events:\n %+v\n", f.FullNode.EVM().LoadEvents(ctx, *ret.Receipt.EventsRoot))
}

func effectiveEthAddressForCreate(t *testing.T, sender address.Address) ethtypes.EthAddress {
	switch sender.Protocol() {
	case address.SECP256K1, address.BLS:
		hasher := sha3.NewLegacyKeccak256()
		hasher.Write(sender.Bytes())
		addr, err := ethtypes.CastEthAddress(hasher.Sum(nil)[12:])
		require.NoError(t, err)
		return addr
	case address.Delegated:
		addr, err := ethtypes.EthAddressFromFilecoinAddress(sender)
		require.NoError(t, err)
		return addr
	default:
		require.FailNow(t, "unsupported protocol %d", sender.Protocol())
	}
	panic("unreachable")
}

func createAndDeploy(ctx context.Context, t *testing.T, client *kit.TestFullNode, fromAddr address.Address, contract []byte) *lapi.MsgLookup {
	// Create and deploy evm actor

	method := builtintypes.MethodsEAM.CreateExternal
	contractParams := abi.CborBytes(contract)
	params, actorsErr := actors.SerializeParams(&contractParams)
	require.NoError(t, actorsErr)

	createMsg := &types.Message{
		To:     builtintypes.EthereumAddressManagerActorAddr,
		From:   fromAddr,
		Value:  big.Zero(),
		Method: method,
		Params: params,
	}
	smsg, err := client.MpoolPushMessage(ctx, createMsg, nil)
	require.NoError(t, err)

	wait, err := client.StateWaitMsg(ctx, smsg.Cid(), 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, exitcode.Ok, wait.Receipt.ExitCode)
	return wait
}

func getEthAddressTX(ctx context.Context, t *testing.T, client *kit.TestFullNode, wait *lapi.MsgLookup, ethAddr ethtypes.EthAddress) ethtypes.EthAddress {
	// Check if eth address returned from CreateExternal is the same as eth address predicted at the start
	var createExternalReturn eam.CreateExternalReturn
	err := createExternalReturn.UnmarshalCBOR(bytes.NewReader(wait.Receipt.Return))
	require.NoError(t, err)

	createdEthAddr, err := ethtypes.CastEthAddress(createExternalReturn.EthAddress[:])
	require.NoError(t, err)
	return createdEthAddr
}
