package modules

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/transport/graphsync"
	datatransferv2 "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/filecoin-project/lotus/node/config"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/storetheindex/dagsync/dtsync"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

type IdxProv struct {
	fx.In

	fx.Lifecycle
	Datastore lotus_dtypes.MetadataDS
}

func IndexProvider(cfg config.IndexProviderConfig) func(params IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr lotus_dtypes.MinerAddress, ps *pubsub.PubSub, nn lotus_dtypes.NetworkName) (provider.Interface, error) {
	if !cfg.Enable {
		log.Warnf("Starting Boost with index provider disabled - no announcements will be made to the index provider")
		return func(params IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr lotus_dtypes.MinerAddress, ps *pubsub.PubSub, nn lotus_dtypes.NetworkName) (provider.Interface, error) {
			return indexprovider.NewDisabledIndexProvider(), nil
		}
	}
	return func(args IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr lotus_dtypes.MinerAddress, ps *pubsub.PubSub, nn lotus_dtypes.NetworkName) (provider.Interface, error) {
		topicName := cfg.TopicName
		// If indexer topic name is left empty, infer it from the network name.
		if topicName == "" {
			// Use the same mechanism as the Dependency Injection (DI) to construct the topic name,
			// so that we are certain it is consistent with the name allowed by the subscription
			// filter.
			//
			// See: lp2p.GossipSub.
			topicName = build.IndexerIngestTopic(dtypes.NetworkName(nn))
			log.Debugw("Inferred indexer topic from network name", "topic", topicName)
		}

		marketHostAddrs := marketHost.Addrs()
		marketHostAddrsStr := make([]string, 0, len(marketHostAddrs))
		for _, a := range marketHostAddrs {
			marketHostAddrsStr = append(marketHostAddrsStr, a.String())
		}

		ipds := namespace.Wrap(args.Datastore, datastore.NewKey("/index-provider"))
		var opts = []engine.Option{
			engine.WithDatastore(ipds),
			engine.WithHost(marketHost),
			engine.WithRetrievalAddrs(marketHostAddrsStr...),
			engine.WithEntriesCacheCapacity(cfg.EntriesCacheCapacity),
			engine.WithChainedEntries(cfg.EntriesChunkSize),
			engine.WithTopicName(topicName),
			engine.WithPurgeCacheOnStart(cfg.PurgeCacheOnStart),
		}

		llog := log.With(
			"idxProvEnabled", cfg.Enable,
			"pid", marketHost.ID(),
			"topic", topicName,
			"retAddrs", marketHost.Addrs())
		// If announcements to the network are enabled, then set options for datatransfer publisher.
		var e *engine.Engine
		if cfg.Enable {
			// Join the indexer topic using the market's pubsub instance. Otherwise, the provider
			// engine would create its own instance of pubsub down the line in go-legs, which has
			// no validators by default.
			t, err := ps.Join(topicName)
			if err != nil {
				llog.Errorw("Failed to join indexer topic", "err", err)
				return nil, xerrors.Errorf("joining indexer topic %s: %w", topicName, err)
			}

			// Get the miner ID and set as extra gossip data.
			// The extra data is required by the lotus-specific index-provider gossip message validators.
			ma := address.Address(maddr)
			opts = append(opts,
				engine.WithPublisherKind(engine.DataTransferPublisher),
				engine.WithDataTransfer(dtV1ToIndexerDT(dt, func() ipld.LinkSystem {
					return *e.LinkSystem()
				})),
				engine.WithExtraGossipData(ma.Bytes()),
				engine.WithTopic(t),
			)
			llog = llog.With("extraGossipData", ma, "publisher", "data-transfer")
		} else {
			opts = append(opts, engine.WithPublisherKind(engine.NoPublisher))
			llog = llog.With("publisher", "none")
		}

		// Instantiate the index provider engine.
		var err error
		e, err = engine.New(opts...)
		if err != nil {
			return nil, xerrors.Errorf("creating indexer provider engine: %w", err)
		}
		llog.Info("Instantiated index provider engine")

		args.Lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				// Note that the OnStart context is cancelled after startup. Its use in e.Start is
				// to start up gossipsub publishers and restore cache, all of  which are completed
				// before e.Start returns. Therefore, it is fine to reuse the give context.
				if err := e.Start(ctx); err != nil {
					return xerrors.Errorf("starting indexer provider engine: %w", err)
				}
				log.Infof("Started index provider engine")
				return nil
			},
			OnStop: func(_ context.Context) error {
				if err := e.Shutdown(); err != nil {
					return xerrors.Errorf("shutting down indexer provider engine: %w", err)
				}
				return nil
			},
		})
		return e, nil
	}
}

// The index provider needs to set up some go-data-transfer voucher code.
// Below we write a shim for the specific use case of index provider, that
// translates between the go-data-transfer v2 use case that the index provider
// implements and the go-data-transfer v1 code that boost imports.
func dtV1ToIndexerDT(dt dtypes.ProviderDataTransfer, linksys func() ipld.LinkSystem) datatransferv2.Manager {
	return &indexerDT{dt: dt, linksys: linksys}
}

type indexerDT struct {
	dt      dtypes.ProviderDataTransfer
	linksys func() ipld.LinkSystem
}

var _ datatransferv2.Manager = (*indexerDT)(nil)

func (i *indexerDT) RegisterVoucherType(voucherType datatransferv2.TypeIdentifier, validator datatransferv2.RequestValidator) error {
	if voucherType == dtsync.LegsVoucherType {
		return i.dt.RegisterVoucherType(&types.LegsVoucherDTv1{}, &dtv1ReqValidator{v: validator})
	}
	return fmt.Errorf("unrecognized voucher type: %s", voucherType)
}

func (i *indexerDT) RegisterTransportConfigurer(voucherType datatransferv2.TypeIdentifier, configurer datatransferv2.TransportConfigurer) error {
	if voucherType == dtsync.LegsVoucherType {
		return i.dt.RegisterTransportConfigurer(&types.LegsVoucherDTv1{}, func(chid datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
			gsTransport, ok := transport.(*graphsync.Transport)
			if ok {
				err := gsTransport.UseStore(chid, i.linksys())
				if err != nil {
					log.Warnf("setting store for legs voucher: %s", err)
				}
			} else {
				log.Warnf("expected transport configurer to pass graphsync transport but got %T", transport)
			}
		})
	}
	return fmt.Errorf("unrecognized voucher type: %s", voucherType)
}

func (i *indexerDT) Start(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) OnReady(readyFunc datatransferv2.ReadyFunc) {
}

func (i *indexerDT) Stop(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) OpenPushDataChannel(ctx context.Context, to peer.ID, voucher datatransferv2.TypedVoucher, baseCid cid.Cid, selector datamodel.Node, options ...datatransferv2.TransferOption) (datatransferv2.ChannelID, error) {
	return datatransferv2.ChannelID{}, fmt.Errorf("not implemented")
}

func (i *indexerDT) OpenPullDataChannel(ctx context.Context, to peer.ID, voucher datatransferv2.TypedVoucher, baseCid cid.Cid, selector datamodel.Node, options ...datatransferv2.TransferOption) (datatransferv2.ChannelID, error) {
	return datatransferv2.ChannelID{}, fmt.Errorf("not implemented")
}

func (i *indexerDT) SendVoucher(ctx context.Context, chid datatransferv2.ChannelID, voucher datatransferv2.TypedVoucher) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) SendVoucherResult(ctx context.Context, chid datatransferv2.ChannelID, voucherResult datatransferv2.TypedVoucher) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) UpdateValidationStatus(ctx context.Context, chid datatransferv2.ChannelID, validationResult datatransferv2.ValidationResult) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) CloseDataTransferChannel(ctx context.Context, chid datatransferv2.ChannelID) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) PauseDataTransferChannel(ctx context.Context, chid datatransferv2.ChannelID) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) ResumeDataTransferChannel(ctx context.Context, chid datatransferv2.ChannelID) error {
	return fmt.Errorf("not implemented")
}

func (i *indexerDT) TransferChannelStatus(ctx context.Context, x datatransferv2.ChannelID) datatransferv2.Status {
	return 0
}

func (i *indexerDT) ChannelState(ctx context.Context, chid datatransferv2.ChannelID) (datatransferv2.ChannelState, error) {
	return nil, fmt.Errorf("not implemented")
}

func (i *indexerDT) SubscribeToEvents(subscriber datatransferv2.Subscriber) datatransferv2.Unsubscribe {
	return func() {}
}

func (i *indexerDT) InProgressChannels(ctx context.Context) (map[datatransferv2.ChannelID]datatransferv2.ChannelState, error) {
	return nil, fmt.Errorf("not implemented")
}

func (i *indexerDT) RestartDataTransferChannel(ctx context.Context, chid datatransferv2.ChannelID) error {
	return fmt.Errorf("not implemented")
}

type dtv1ReqValidator struct {
	v datatransferv2.RequestValidator
}

func (d *dtv1ReqValidator) ValidatePush(isRestart bool, chid datatransfer.ChannelID, sender peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	d2v := dtsync.BindnodeRegistry.TypeToNode(&voucher.(*types.LegsVoucherDTv1).Voucher)
	res, err := d.v.ValidatePush(toChannelIDV2(chid), sender, d2v, baseCid, selector)
	if err != nil {
		return nil, err
	}
	if !res.Accepted {
		return nil, datatransfer.ErrRejected
	}

	return toVoucherResult(res)
}

func (d *dtv1ReqValidator) ValidatePull(isRestart bool, chid datatransfer.ChannelID, receiver peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	d2v := dtsync.BindnodeRegistry.TypeToNode(&voucher.(*types.LegsVoucherDTv1).Voucher)
	res, err := d.v.ValidatePull(toChannelIDV2(chid), receiver, d2v, baseCid, selector)
	if err != nil {
		return nil, err
	}
	if !res.Accepted {
		return nil, datatransfer.ErrRejected
	}

	return toVoucherResult(res)
}

func toVoucherResult(res datatransferv2.ValidationResult) (datatransfer.VoucherResult, error) {
	voucherResVoucher := res.VoucherResult.Voucher
	vri, err := dtsync.BindnodeRegistry.TypeFromNode(voucherResVoucher, &dtsync.VoucherResult{})
	if err != nil {
		return nil, fmt.Errorf("getting VoucherResult from ValidationResult: %w", err)
	}
	vr := vri.(*dtsync.VoucherResult)
	if vr == nil {
		return nil, fmt.Errorf("got nil VoucherResult from ValidationResult")
	}
	return &types.LegsVoucherResultDtv1{VoucherResult: *vr, VoucherType: res.VoucherResult.Type}, nil
}

func toChannelIDV2(chid datatransfer.ChannelID) datatransferv2.ChannelID {
	return datatransferv2.ChannelID{
		Initiator: chid.Initiator,
		Responder: chid.Responder,
		ID:        datatransferv2.TransferID(chid.ID),
	}
}
