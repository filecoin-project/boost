package dtypes

import (
	graphsync "github.com/filecoin-project/boost-graphsync"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-statestore"
	bserv "github.com/ipfs/boxo/blockservice"
	exchange "github.com/ipfs/boxo/exchange"
	"github.com/ipfs/go-datastore"

	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/storagemarket/impl/requestvalidation"
	ipfsblockstore "github.com/ipfs/boxo/blockstore"

	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/node/repo/imports"
)

type (
	// UniversalBlockstore is the universal blockstore backend.
	UniversalBlockstore blockstore.Blockstore

	// ColdBlockstore is the Cold blockstore abstraction for the splitstore
	ColdBlockstore blockstore.Blockstore

	// HotBlockstore is the Hot blockstore abstraction for the splitstore
	HotBlockstore blockstore.Blockstore

	// SplitBlockstore is the hot/cold blockstore that sits on top of the ColdBlockstore.
	SplitBlockstore blockstore.Blockstore

	// BaseBlockstore is something, coz DI
	BaseBlockstore blockstore.Blockstore

	// BasicChainBlockstore is like ChainBlockstore, but without the optional
	// network fallback support
	BasicChainBlockstore blockstore.Blockstore

	// ChainBlockstore is a blockstore to store chain data (tipsets, blocks,
	// messages). It is physically backed by the BareMonolithBlockstore, but it
	// has a cache on top that is specially tuned for chain data access
	// patterns.
	ChainBlockstore blockstore.Blockstore

	// BasicStateBlockstore is like StateBlockstore, but without the optional
	// network fallback support
	BasicStateBlockstore blockstore.Blockstore

	// StateBlockstore is a blockstore to store state data (state tree). It is
	// physically backed by the BareMonolithBlockstore, but it has a cache on
	// top that is specially tuned for state data access patterns.
	StateBlockstore blockstore.Blockstore

	// ExposedBlockstore is a blockstore that interfaces directly with the
	// network or with users, from which queries are served, and where incoming
	// data is deposited. For security reasons, this store is disconnected from
	// any internal caches. If blocks are added to this store in a way that
	// could render caches dirty (e.g. a block is added when an existence cache
	// holds a 'false' for that block), the process should signal so by calling
	// blockstore.AllCaches.Dirty(cid).
	ExposedBlockstore blockstore.Blockstore

	// IndexBackedBlockstore is an abstraction on top of the DAGStore that provides
	// access to any CID in a free or unsealed sector
	IndexBackedBlockstore ipfsblockstore.Blockstore
)

type ChainBitswap exchange.Interface
type ChainBlockService bserv.BlockService

type ClientImportMgr *imports.Manager
type ClientBlockstore blockstore.BasicBlockstore
type ClientDealStore *statestore.StateStore
type ClientRequestValidator *requestvalidation.UnifiedRequestValidator
type ClientDatastore datastore.Batching

type Graphsync graphsync.GraphExchange

// ClientDataTransfer is a data transfer manager for the client
type ClientDataTransfer datatransfer.Manager
type ProviderDataTransfer datatransfer.Manager
type ProviderTransferNetwork dtnet.DataTransferNetwork
type ProviderTransport datatransfer.Transport

type ProviderDealStore *statestore.StateStore
type ProviderPieceStore piecestore.PieceStore

type ProviderRequestValidator *requestvalidation.UnifiedRequestValidator

type StagingBlockstore blockstore.BasicBlockstore
type StagingGraphsync graphsync.GraphExchange
