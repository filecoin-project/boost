package car

import (
	"errors"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestRefCountBICM(t *testing.T) {
	bicm := NewRefCountBICM()
	testRefCountBICM(t, bicm, bicm.cache)
}

func testRefCountBICM(t *testing.T, bicm BlockInfoCacheManager, internalCache map[cid.Cid]*cacheRefs) {
	payloadCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)

	bic := bicm.Get(payloadCid)
	bic2 := bicm.Get(payloadCid)
	require.True(t, bic == bic2)
	require.Len(t, internalCache, 1)

	// Unref the cache once, should still be a copy in memory
	bicm.Unref(payloadCid, nil)
	require.Len(t, internalCache, 1)

	// Unref the cache a second time, there should now be no copies in memory
	bicm.Unref(payloadCid, nil)
	require.Len(t, internalCache, 0)
}

func TestDelayedUnrefBICM(t *testing.T) {
	t.Run("test ref counting: unref with nil error", func(t *testing.T) {
		bicm := NewDelayedUnrefBICM(100 * time.Millisecond)
		testRefCountBICM(t, bicm, bicm.cache)
	})

	t.Run("test ref counting: each unref with an error", func(t *testing.T) {
		duration := 100 * time.Millisecond
		bicm := NewDelayedUnrefBICM(duration)
		internalCache := bicm.cache

		payloadCid, err := cid.Parse("bafkqaaa")
		require.NoError(t, err)

		bic := bicm.Get(payloadCid)
		bic2 := bicm.Get(payloadCid)
		require.True(t, bic == bic2)
		require.Len(t, internalCache, 1)

		// Unref the cache twice, with an error each time.
		// Expect the cache not to be cleared until the timer has expired on
		// the last unreference.
		bicm.Unref(payloadCid, errors.New("err"))
		time.Sleep(duration / 2)
		bicm.Unref(payloadCid, errors.New("err"))

		// total sleep:
		// first ref - 0.5 timer duration
		// second ref - 0 timer duration
		require.Len(t, internalCache, 1)

		time.Sleep(duration/2 + duration/4)
		// total sleep:
		// first ref - 1.25 timer duration
		// second ref - 0.75 timer duration
		require.Len(t, internalCache, 1)

		time.Sleep(duration)
		// total sleep:
		// first ref - 2.25 timer duration
		// second ref - 1.75 timer duration
		require.Len(t, internalCache, 0)
	})

	t.Run("test ref counting: unref with error, then unref with no error", func(t *testing.T) {
		duration := 100 * time.Millisecond
		bicm := NewDelayedUnrefBICM(duration)
		internalCache := bicm.cache

		payloadCid, err := cid.Parse("bafkqaaa")
		require.NoError(t, err)

		bic := bicm.Get(payloadCid)
		bic2 := bicm.Get(payloadCid)
		require.True(t, bic == bic2)
		require.Len(t, internalCache, 1)

		// Unref the cache twice, the first with an error, the second without
		// an error
		bicm.Unref(payloadCid, errors.New("err"))
		bicm.Unref(payloadCid, nil)

		// Expect the cache not to be cleared until the timer has expired on the
		// unref with an error
		require.Len(t, internalCache, 1)
		time.Sleep(duration / 2)
		require.Len(t, internalCache, 1)
		time.Sleep(duration)
		require.Len(t, internalCache, 0)
	})

	t.Run("test ref counting: unref with no error, then unref with error", func(t *testing.T) {
		duration := 100 * time.Millisecond
		bicm := NewDelayedUnrefBICM(duration)
		internalCache := bicm.cache

		payloadCid, err := cid.Parse("bafkqaaa")
		require.NoError(t, err)

		bic := bicm.Get(payloadCid)
		bic2 := bicm.Get(payloadCid)
		require.True(t, bic == bic2)
		require.Len(t, internalCache, 1)

		// Unref the cache twice, the first with no error, the second with
		// an error
		bicm.Unref(payloadCid, nil)
		bicm.Unref(payloadCid, errors.New("err"))

		// Expect the cache not to be cleared until the timer has expired on the
		// unref with an error
		require.Len(t, internalCache, 1)
		time.Sleep(duration / 2)
		require.Len(t, internalCache, 1)
		time.Sleep(duration)
		require.Len(t, internalCache, 0)
	})
}
