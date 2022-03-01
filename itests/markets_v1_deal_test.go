package itests

import (
	"testing"

	"github.com/filecoin-project/boost/testutil"
	"github.com/stretchr/testify/require"

	lapi "github.com/filecoin-project/lotus/api"
)

func TestMarketsV1Deal(t *testing.T) {
	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	rseed := 0
	size := 7 << 20 // 7MiB file

	path, err := testutil.CreateRandomFile(t.TempDir(), rseed, size)
	require.NoError(t, err)
	res, err := f.fullNode.ClientImport(f.ctx, lapi.FileRef{Path: path})
	require.NoError(t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root

	dealProposalCid, err := f.fullNode.ClientStartDeal(f.ctx, &dp)
	require.NoError(t, err)

	err = f.WaitDealSealed(f.ctx, dealProposalCid)
	require.NoError(t, err)

	//TODO: confirm retrieval works
}
