package itests

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/itests/shared"
	"github.com/filecoin-project/lotus/itests/kit"
)

func TestMultiMinerRetrievalGraphsync(t *testing.T) {
	shared.RunMultiminerRetrievalTest(t, func(ctx context.Context, t *testing.T, rt *shared.RetrievalTest) {
		for i, rootCid := range rt.RootCids {
			// The deal was stored on the first boost's miner.
			// Retrieve the deal from the second boost. It should
			// - get the index of the piece's block offsets from LID
			// - get the deal info from LID
			// - recognize that the deal is for a sector on the first miner
			// - read the data for the deal from the first miner
			t.Logf("deal is added to piece, starting retrieval of root %s", rootCid)
			outPath := rt.BoostAndMiners.RetrieveDirect(ctx, t, rootCid, nil, true, nil, rt.BoostAndMiners.MinerAddrs[0])

			sampleFilePath := rt.SampleFilePaths[i]
			t.Logf("retrieval is done, compare in- and out- files in: %s, out: %s", sampleFilePath, outPath)
			kit.AssertFilesEqual(t, sampleFilePath, outPath)
		}
	})
}
