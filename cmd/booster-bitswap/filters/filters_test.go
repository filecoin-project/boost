package filters_test

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/filters"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestMultiFilter(t *testing.T) {
	peer1, err := peer.Decode("Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi")
	require.NoError(t, err)
	peer2, err := peer.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	require.NoError(t, err)
	peer3, err := peer.Decode("QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP")
	require.NoError(t, err)
	blockedCid1, err := cid.Parse("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	require.NoError(t, err)
	blockedCid2, err := cid.Parse("QmTn7prGSqKUd7cqvAjnULrH7zxBEBWrnj9kE7kZSGtDuQ")
	require.NoError(t, err)
	notBlockedCid, err := cid.Parse("QmajLDwZLH6bKTzd8jkq913ZbxaB2nFGRrkDAuygYNNv39")
	require.NoError(t, err)
	tickChan := make(chan struct{}, 1)
	onTick := func() {
		tickChan <- struct{}{}
	}
	fbf := &fakeBlockFetcher{}
	fpf := &fakePeerFetcher{}
	clock := clock.NewMock()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	cfgDir, err := os.MkdirTemp("", "filters")
	require.NoError(t, err)
	mf := filters.NewMultiFilterWithConfigs(cfgDir, []filters.FilterConfig{
		{
			CacheFile: filepath.Join(cfgDir, "denylist.json"),
			Fetcher:   fbf.fetchDenyList,
			Handler:   filters.NewBlockFilter(),
		},
		{
			CacheFile: filepath.Join(cfgDir, "remoteconfig.json"),
			Fetcher:   fpf.fetchList,
			Handler:   filters.NewRemoteConfigFilter(&testBandwidthMeasure{}),
		},
	}, clock, onTick)
	err = mf.Start(ctx)
	require.NoError(t, err)
	cache, err := os.ReadFile(filepath.Join(cfgDir, "denylist.json"))
	require.NoError(t, err)
	require.Equal(t, `[
		{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"}
	]
	`, string(cache))
	cache, err = os.ReadFile(filepath.Join(cfgDir, "remoteconfig.json"))
	require.NoError(t, err)
	require.Equal(t, `{
		"AllowDenyList": {
				"Type": "allowlist", 
				"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
		}
	}`, string(cache))
	ss := filters.ServerState{}
	// blockedCid1 is blocked, do not fulfill
	fulfillRequest, err := mf.FulfillRequest(peer1, blockedCid1, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// blockedCid2 is not blocked, peer1 is allowed, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer1, blockedCid2, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// blockedCid2 is not blocked, peer2 is allowed, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer2, blockedCid2, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// blockedCid2 is not blocked, peer3 is not allowed, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid2, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-tickChan:
	}
	clock.Add(filters.UpdateInterval)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-tickChan:
	}
	// blockedCid1 is blocked, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer1, blockedCid1, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// blockedCid2 is not blocked, peer1 is allowed, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer1, blockedCid2, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// blockedCid2 is not blocked, peer2 is allowed, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer2, blockedCid2, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// blockedCid2 is not blocked, peer3 is not allowed, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid2, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	clock.Add(filters.UpdateInterval)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-tickChan:
	}
	// blockedCid1 is blocked, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid1, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// blockedCid2 is now blocked, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid2, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// notBlockedCid is not blocked, peer3 is not denied, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, notBlockedCid, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// notBlockedCid is not blocked, peer1 is denied, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer1, notBlockedCid, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// notBlockedCid is not blocked, peer2 is denied, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer2, notBlockedCid, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	cache, err = os.ReadFile(filepath.Join(cfgDir, "denylist.json"))
	require.NoError(t, err)
	require.Equal(t, `[
			{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"},
			{ "anchor": "6a98dfc49e852da7eee32d7df49801cb3ae7a432aa73200cd652ba149272481a"}
		]
		`, string(cache))
	cache, err = os.ReadFile(filepath.Join(cfgDir, "remoteconfig.json"))
	require.NoError(t, err)
	require.Equal(t, `{
			"AllowDenyList": {
					"Type": "denylist", 
					"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
			}
		}`, string(cache))

	// now restart a new instance, with a fetcher that always errors,
	// and verify disk cache works
	mf.Close()
	mf = filters.NewMultiFilterWithConfigs(cfgDir, []filters.FilterConfig{
		{
			CacheFile: filepath.Join(cfgDir, "denylist.json"),
			Fetcher: func(time.Time) (bool, io.ReadCloser, error) {
				return false, nil, errors.New("something went wrong")
			},
			Handler: filters.NewBlockFilter(),
		},
		{
			CacheFile: filepath.Join(cfgDir, "remoteconfig.json"),
			Fetcher:   fpf.fetchList,
			Handler:   filters.NewRemoteConfigFilter(&testBandwidthMeasure{}),
		},
	}, clock, onTick)
	err = mf.Start(ctx)
	require.NoError(t, err)
	// blockedCid1 is blocked, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid1, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// blockedCid2 is now blocked, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, blockedCid2, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// notBlockedCid is not blocked, peer3 is not denied, fulfill
	fulfillRequest, err = mf.FulfillRequest(peer3, notBlockedCid, ss)
	require.NoError(t, err)
	require.True(t, fulfillRequest)
	// notBlockedCid is not blocked, peer1 is denied, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer1, notBlockedCid, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
	// notBlockedCid is not blocked, peer2 is denied, do not fulfill
	fulfillRequest, err = mf.FulfillRequest(peer2, notBlockedCid, ss)
	require.NoError(t, err)
	require.False(t, fulfillRequest)
}

type fakeBlockFetcher struct {
	fetchCount int
}

func (fbf *fakeBlockFetcher) fetchDenyList(fetchTime time.Time) (bool, io.ReadCloser, error) {
	denyList := `[
		{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"}
	]
	`
	updated := true
	if fbf.fetchCount == 1 {
		updated = false
	}
	if fbf.fetchCount > 1 {
		denyList = `[
			{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"},
			{ "anchor": "6a98dfc49e852da7eee32d7df49801cb3ae7a432aa73200cd652ba149272481a"}
		]
		`
	}
	fbf.fetchCount++
	return updated, ioutil.NopCloser(strings.NewReader(denyList)), nil
}

type fakePeerFetcher struct {
	fetchCount int
}

func (fpf *fakePeerFetcher) fetchList(fetchTime time.Time) (bool, io.ReadCloser, error) {
	list := `{
		"AllowDenyList": {
				"Type": "allowlist", 
				"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
		}
	}`
	updated := true
	if fpf.fetchCount == 1 {
		updated = false
	}
	if fpf.fetchCount > 1 {
		list = `{
			"AllowDenyList": {
					"Type": "denylist", 
					"PeerIDs": ["Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi", "QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N"]
			}
		}`
	}
	fpf.fetchCount++
	return updated, ioutil.NopCloser(strings.NewReader(list)), nil
}
