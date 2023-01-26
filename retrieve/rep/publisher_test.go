package rep_test

import (
	"context"
	"testing"
	"time"

	"github.com/application-research/filclient/rep"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var testCid1 = mustCid("bafybeihrqe2hmfauph5yfbd6ucv7njqpiy4tvbewlvhzjl4bhnyiu6h7pm")

type sub struct {
	ping chan rep.RetrievalEvent
}

func (us *sub) OnRetrievalEvent(evt rep.RetrievalEvent) {
	if us.ping != nil {
		us.ping <- evt
	}
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	pub := rep.New(context.Background())
	sub := &sub{}
	require.Equal(t, 0, pub.SubscriberCount(), "has no subscribers")

	unsub := pub.Subscribe(sub)
	unsub2 := pub.Subscribe(sub)

	require.Equal(t, 2, pub.SubscriberCount(), "registered both subscribers")
	unsub()
	require.Equal(t, 1, pub.SubscriberCount(), "unregistered first subscriber")
	unsub2()
	require.Equal(t, 0, pub.SubscriberCount(), "unregistered second subscriber")
}

func TestEventing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	pub := rep.New(ctx)
	sub := &sub{ping: make(chan rep.RetrievalEvent)}

	pub.Subscribe(sub)
	pid := peer.NewPeerRecord().PeerID
	pub.Publish(rep.NewRetrievalEventConnect(rep.QueryPhase, testCid1, pid, address.Undef))
	pub.Publish(rep.NewRetrievalEventSuccess(rep.RetrievalPhase, testCid1, "", address.TestAddress, 101, 202, time.Millisecond*303, abi.NewTokenAmount(404)))

	evt := <-sub.ping
	require.Equal(t, rep.QueryPhase, evt.Phase())
	require.Equal(t, testCid1, evt.PayloadCid())
	require.Equal(t, pid, evt.StorageProviderId())
	require.Equal(t, address.Undef, evt.StorageProviderAddr())
	require.Equal(t, rep.ConnectedCode, evt.Code())
	_, ok := evt.(rep.RetrievalEventConnect)
	require.True(t, ok)

	evt = <-sub.ping
	require.Equal(t, rep.RetrievalPhase, evt.Phase())
	require.Equal(t, testCid1, evt.PayloadCid())
	require.Equal(t, peer.ID(""), evt.StorageProviderId())
	require.Equal(t, address.TestAddress, evt.StorageProviderAddr())
	require.Equal(t, rep.SuccessCode, evt.Code())
	res, ok := evt.(rep.RetrievalEventSuccess)
	require.True(t, ok)
	require.Equal(t, uint64(101), res.ReceivedSize())
	require.Equal(t, int64(202), res.ReceivedCids())
	require.Equal(t, time.Millisecond*303, res.Duration())
	require.Equal(t, abi.NewTokenAmount(404), res.TotalPayment())
}

func mustCid(cstr string) cid.Cid {
	c, err := cid.Decode(cstr)
	if err != nil {
		panic(err)
	}
	return c
}
