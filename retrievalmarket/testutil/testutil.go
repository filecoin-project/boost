package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/filecoin-project/boost-graphsync/storeutil"
	dtnet "github.com/filecoin-project/boost/datatransfer/network"
	"github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

type Libp2pTestData struct {
	Ctx         context.Context
	Ds1         datastore.Batching
	Ds2         datastore.Batching
	Bs1         bstore.Blockstore
	Bs2         bstore.Blockstore
	DagService1 ipldformat.DAGService
	DagService2 ipldformat.DAGService
	DTNet1      dtnet.DataTransferNetwork
	DTNet2      dtnet.DataTransferNetwork
	DTStore1    datastore.Batching
	DTStore2    datastore.Batching
	DTTmpDir1   string
	DTTmpDir2   string
	LinkSystem1 ipld.LinkSystem
	LinkSystem2 ipld.LinkSystem
	Host1       host.Host
	Host2       host.Host
	OrigBytes   []byte

	MockNet mocknet.Mocknet
}

func NewLibp2pTestData(ctx context.Context, t *testing.T) *Libp2pTestData {
	testData := &Libp2pTestData{}
	testData.Ctx = ctx

	var err error

	testData.Ds1 = dss.MutexWrap(datastore.NewMapDatastore())
	testData.Ds2 = dss.MutexWrap(datastore.NewMapDatastore())

	// make a bstore and dag service
	testData.Bs1 = bstore.NewBlockstore(testData.Ds1)
	testData.Bs2 = bstore.NewBlockstore(testData.Ds2)

	testData.DagService1 = merkledag.NewDAGService(blockservice.New(testData.Bs1, offline.Exchange(testData.Bs1)))
	testData.DagService2 = merkledag.NewDAGService(blockservice.New(testData.Bs2, offline.Exchange(testData.Bs2)))

	// setup an IPLD link system for bstore 1
	testData.LinkSystem1 = storeutil.LinkSystemForBlockstore(testData.Bs1)

	// setup an IPLD link system for bstore 2
	testData.LinkSystem2 = storeutil.LinkSystemForBlockstore(testData.Bs2)

	mn := mocknet.New()

	// setup network
	testData.Host1, err = mn.GenPeer()
	require.NoError(t, err)

	testData.Host2, err = mn.GenPeer()
	require.NoError(t, err)

	err = mn.LinkAll()
	require.NoError(t, err)

	testData.DTNet1 = dtnet.NewFromLibp2pHost(testData.Host1)
	testData.DTNet2 = dtnet.NewFromLibp2pHost(testData.Host2)

	testData.DTStore1 = namespace.Wrap(testData.Ds1, datastore.NewKey("DataTransfer1"))
	testData.DTStore2 = namespace.Wrap(testData.Ds1, datastore.NewKey("DataTransfer2"))

	testData.DTTmpDir1, err = os.MkdirTemp("", "dt-tmp-1")
	require.NoError(t, err)
	testData.DTTmpDir2, err = os.MkdirTemp("", "dt-tmp-2")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(testData.DTTmpDir1)
		_ = os.RemoveAll(testData.DTTmpDir2)
	})

	testData.MockNet = mn

	return testData
}
