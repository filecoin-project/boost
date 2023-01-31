package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/retrieve"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/term"
)

type RetrievalStats interface {
	GetByteSize() uint64
	GetDuration() time.Duration
	GetAverageBytesPerSecond() uint64
}

type FILRetrievalStats struct {
	retrieve.RetrievalStats
}

func (stats *FILRetrievalStats) GetByteSize() uint64 {
	return stats.Size
}

func (stats *FILRetrievalStats) GetDuration() time.Duration {
	return stats.Duration
}

func (stats *FILRetrievalStats) GetAverageBytesPerSecond() uint64 {
	return stats.AverageSpeed
}

func totalCost(qres *retrievalmarket.QueryResponse) big.Int {
	return big.Add(big.Mul(qres.MinPricePerByte, big.NewIntUnsigned(qres.Size)), qres.UnsealPrice)
}

func printProgress(bytesReceived uint64) {
	str := fmt.Sprintf("%v (%v)", bytesReceived, humanize.IBytes(bytesReceived))

	termWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	strLen := len(str)
	if err == nil {

		if strLen < termWidth {
			// If the string is shorter than the terminal width, pad right side
			// with spaces to remove old text
			str = strings.Join([]string{str, strings.Repeat(" ", termWidth-strLen)}, "")
		} else if strLen > termWidth {
			// If the string doesn't fit in the terminal, cut it down to a size
			// that fits
			str = str[:termWidth]
		}
	}

	fmt.Fprintf(os.Stderr, "%s\r", str)
}

func printRetrievalStats(stats RetrievalStats) {
	switch stats := stats.(type) {
	case *FILRetrievalStats:
		fmt.Printf(`RETRIEVAL STATS (FIL)
-----
Size:          %v (%v)
Duration:      %v
Average Speed: %v (%v/s)
Ask Price:     %v (%v)
Total Payment: %v (%v)
Num Payments:  %v
Peer:          %v
`,
			stats.Size, humanize.IBytes(stats.Size),
			stats.Duration,
			stats.AverageSpeed, humanize.IBytes(stats.AverageSpeed),
			stats.AskPrice, types.FIL(stats.AskPrice),
			stats.TotalPayment, types.FIL(stats.TotalPayment),
			stats.NumPayments,
			stats.Peer,
		)
	}
}
