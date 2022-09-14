package modules

import (
	mdagstore "github.com/filecoin-project/boost/node/modules/dagstore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

// RetrievalProvider creates a new retrieval provider attached to the provider blockstore
func RetrievalProvider(
	maddr lotus_dtypes.MinerAddress,
	adapter retrievalmarket.RetrievalProviderNode,
	sa retrievalmarket.SectorAccessor,
	netwk rmnet.RetrievalMarketNetwork,
	ds lotus_dtypes.MetadataDS,
	pieceStore lotus_dtypes.ProviderPieceStore,
	dt dtypes.ProviderDataTransfer,
	pricingFnc lotus_dtypes.RetrievalPricingFunc,
	userFilter lotus_dtypes.RetrievalDealFilter,
	dagStore *mdagstore.Wrapper,
) (retrievalmarket.RetrievalProvider, error) {
	opt := retrievalimpl.DealDeciderOpt(retrievalimpl.DealDecider(userFilter))

	retrievalmarket.DefaultPricePerByte = big.Zero() // todo: for whatever reason this is a global var in markets

	return retrievalimpl.NewProvider(
		address.Address(maddr),
		adapter,
		sa,
		netwk,
		pieceStore,
		dagStore,
		dt,
		namespace.Wrap(ds, datastore.NewKey("/retrievals/provider")),
		retrievalimpl.RetrievalPricingFunc(pricingFnc),
		opt,
	)
}
