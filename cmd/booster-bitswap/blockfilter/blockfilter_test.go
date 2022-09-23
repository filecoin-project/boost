package blockfilter

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestBlockFilter(t *testing.T) {
	blockedCid1, err := cid.Parse("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	require.NoError(t, err)
	blockedCid2, err := cid.Parse("QmTn7prGSqKUd7cqvAjnULrH7zxBEBWrnj9kE7kZSGtDuQ")
	require.NoError(t, err)
	timerSetChan := make(chan struct{}, 1)
	onTimerSet := func() {
		timerSetChan <- struct{}{}
	}
	ff := &fakeFetcher{}
	clock := clock.NewMock()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	bf := newBlockFilter(ff.fetchDenyList, clock, onTimerSet)
	bf.Start(ctx)
	isFiltered, err := bf.IsFiltered(blockedCid1)
	require.NoError(t, err)
	require.True(t, isFiltered)
	isFiltered, err = bf.IsFiltered(blockedCid2)
	require.NoError(t, err)
	require.False(t, isFiltered)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-timerSetChan:
	}
	clock.Add(UpdateInterval)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-timerSetChan:
	}
	isFiltered, err = bf.IsFiltered(blockedCid1)
	require.NoError(t, err)
	require.True(t, isFiltered)
	isFiltered, err = bf.IsFiltered(blockedCid2)
	require.NoError(t, err)
	require.False(t, isFiltered)
	clock.Add(UpdateInterval)
	select {
	case <-ctx.Done():
		t.Fatal("should have updated list but didn't")
	case <-timerSetChan:
	}
	isFiltered, err = bf.IsFiltered(blockedCid1)
	require.NoError(t, err)
	require.True(t, isFiltered)
	isFiltered, err = bf.IsFiltered(blockedCid2)
	require.NoError(t, err)
	require.True(t, isFiltered)
}

type fakeFetcher struct {
	fetchCount int
}

func (ff *fakeFetcher) fetchDenyList(fetchTime time.Time) (bool, io.ReadCloser, error) {
	denyList := `[
		{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"}
	]
	`
	updated := true
	if ff.fetchCount == 1 {
		updated = false
	}
	if ff.fetchCount > 1 {
		denyList = `[
			{ "anchor": "09770fe7ec3124653c1d8f6917e3cd72cbd58a3e24a734bc362f656844c4ee7d"},
			{ "anchor": "6a98dfc49e852da7eee32d7df49801cb3ae7a432aa73200cd652ba149272481a"}
		]
		`
	}
	ff.fetchCount++
	return updated, ioutil.NopCloser(strings.NewReader(denyList)), nil
}
