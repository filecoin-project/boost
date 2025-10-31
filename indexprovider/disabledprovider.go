package indexprovider

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/peer"
)

type DisabledIndexProvider struct {
}

var _ provider.Interface = (*DisabledIndexProvider)(nil)

func NewDisabledIndexProvider() *DisabledIndexProvider {
	return &DisabledIndexProvider{}
}

func (d DisabledIndexProvider) PublishLocal(ctx context.Context, advertisement schema.Advertisement) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("could not publish locally: index provider disabled")
}

func (d DisabledIndexProvider) Publish(ctx context.Context, advertisement schema.Advertisement) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("could not publish: index provider disabled")
}

func (d DisabledIndexProvider) RegisterMultihashLister(lister provider.MultihashLister) {
}

func (d DisabledIndexProvider) NotifyPut(ctx context.Context, provider *peer.AddrInfo, contextID []byte, md metadata.Metadata) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("could not notify put: index provider disabled")
}

func (d DisabledIndexProvider) NotifyRemove(ctx context.Context, providerID peer.ID, contextID []byte) (cid.Cid, error) {
	return cid.Undef, fmt.Errorf("could not notify remove: index provider disabled")
}

func (d DisabledIndexProvider) GetAdv(ctx context.Context, cid cid.Cid) (*schema.Advertisement, error) {
	return nil, fmt.Errorf("could not get advertisement: index provider disabled")
}

func (d DisabledIndexProvider) GetLatestAdv(ctx context.Context) (cid.Cid, *schema.Advertisement, error) {
	return cid.Undef, nil, fmt.Errorf("could not get latest advertisement: index provider disabled")
}

func (d DisabledIndexProvider) Shutdown() error {
	return nil
}
