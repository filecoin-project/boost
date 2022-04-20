package dealfilter

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"

	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/storagemarket/types"
)

const agent = "boost"
const jsonVersion = "2.0.0"

func CliStorageDealFilter(cmd string) dtypes.StorageDealFilter {
	return func(ctx context.Context, deal types.DealFilterParams) (bool, string, error) {
		d := struct {
			types.DealParams
			DealType      string
			FormatVersion string
			Agent         string
		}{
			DealParams:    *deal.DealParams,
			DealType:      "storage",
			FormatVersion: jsonVersion,
			Agent:         agent,
		}
		return runDealFilter(ctx, cmd, d)
	}
}

func CliRetrievalDealFilter(cmd string) dtypes.RetrievalDealFilter {
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
