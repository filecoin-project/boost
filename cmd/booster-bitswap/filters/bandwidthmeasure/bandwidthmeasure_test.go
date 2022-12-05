package bandwidthmeasure_test

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/filters/bandwidthmeasure"
	"github.com/stretchr/testify/require"
)

func TestBandwidthMeasure(t *testing.T) {
	type step struct {
		advanceClock      time.Duration
		bytesSent         uint64
		avgBytesPerSecond uint64
	}
	megabyte := uint64(1 << 20)
	testCases := []struct {
		name         string
		samplePeried time.Duration
		steps        []step
	}{
		{
			name:         "Per second limit 5 mb",
			samplePeried: time.Second,
			steps: []step{
				{
					advanceClock:      0,
					bytesSent:         1 * megabyte,
					avgBytesPerSecond: 1 * megabyte,
				},
				{
					advanceClock:      time.Second / 2,
					bytesSent:         4*megabyte + 1,
					avgBytesPerSecond: 5*megabyte + 1,
				},
				{
					advanceClock:      time.Second/2 + 1,
					bytesSent:         0,
					avgBytesPerSecond: 4*megabyte + 1,
				},
				{
					advanceClock:      time.Second / 2,
					bytesSent:         5 * megabyte,
					avgBytesPerSecond: 5 * megabyte,
				},
			},
		},
		{
			name:         "5 second sample limit 2 mb",
			samplePeried: 5 * time.Second,
			steps: []step{
				{
					advanceClock:      0,
					bytesSent:         1 * megabyte,
					avgBytesPerSecond: megabyte / 5,
				},
				{
					advanceClock:      time.Second,
					bytesSent:         4 * megabyte,
					avgBytesPerSecond: megabyte,
				},
				{
					advanceClock:      time.Second,
					bytesSent:         3 * megabyte,
					avgBytesPerSecond: 8 * megabyte / 5,
				},
				{
					advanceClock:      time.Second,
					bytesSent:         3 * megabyte,
					avgBytesPerSecond: 11 * megabyte / 5,
				},
				{
					advanceClock:      time.Second,
					bytesSent:         0,
					avgBytesPerSecond: 11 * megabyte / 5,
				},
				{
					advanceClock:      time.Second + 1,
					bytesSent:         0,
					avgBytesPerSecond: 2 * megabyte,
				},
				{
					advanceClock:      time.Second,
					bytesSent:         5 * megabyte,
					avgBytesPerSecond: 11 * megabyte / 5,
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			clock := clock.NewMock()
			bm := bandwidthmeasure.NewBandwidthMeasure(testCase.samplePeried, clock)
			for _, step := range testCase.steps {
				clock.Add(step.advanceClock)
				if step.bytesSent > 0 {
					bm.RecordBytesSent(step.bytesSent)
				}
				require.Equal(t, step.avgBytesPerSecond, bm.AvgBytesPerSecond())
			}
		})
	}

}
