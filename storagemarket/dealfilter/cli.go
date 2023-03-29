package dealfilter

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

const agent = "boost"
const jsonVersion = "2.2.0"

type StorageDealFilter func(ctx context.Context, deal DealFilterParams) (bool, string, error)
type RetrievalDealFilter func(ctx context.Context, deal retrievalmarket.ProviderDealState) (bool, string, error)

func CliStorageDealFilter(cmd string) StorageDealFilter {
	return func(ctx context.Context, deal DealFilterParams) (bool, string, error) {
		d := struct {
			types.DealParams
			SealingPipelineState sealingpipeline.Status
			FundsState           funds.Status
			StorageState         storagespace.Status
			DealType             string
			FormatVersion        string
			Agent                string
		}{
			DealParams:           deal.DealParams,
			SealingPipelineState: deal.SealingPipelineState,
			FundsState:           deal.FundsState,
			StorageState:         deal.StorageState,
			DealType:             "storage",
			FormatVersion:        jsonVersion,
			Agent:                agent,
		}
		return runDealFilter(ctx, cmd, d)
	}
}

func CliRetrievalDealFilter(cmd string) RetrievalDealFilter {
	return func(ctx context.Context, deal retrievalmarket.ProviderDealState) (bool, string, error) {
		d := struct {
			retrievalmarket.ProviderDealState
			DealType      string
			FormatVersion string
			Agent         string
		}{
			ProviderDealState: deal,
			DealType:          "retrieval",
			FormatVersion:     jsonVersion,
			Agent:             agent,
		}
		return runDealFilter(ctx, cmd, d)
	}
}

func runDealFilter(ctx context.Context, cmd string, deal interface{}) (bool, string, error) {
	j, err := json.MarshalIndent(deal, "", "  ")
	if err != nil {
		return false, "", err
	}

	var out bytes.Buffer

	c := exec.Command("sh", "-c", cmd)
	c.Stdin = bytes.NewReader(j)
	c.Stdout = &out
	c.Stderr = &out

	switch err := c.Run().(type) {
	case nil:
		return true, "", nil
	case *exec.ExitError:
		return false, out.String(), nil
	default:
		return false, "filter cmd run error", err
	}
}
