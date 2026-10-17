package dealfilter

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

const agent = "boost"

// The two payloads go to different filters, so each carries its own version.
// The storage payload gained a field, which a filter decoding it strictly sees.
const (
	storageFilterVersion   = "2.3.0"
	retrievalFilterVersion = "2.2.0"
)

type StorageDealFilter func(ctx context.Context, deal DealFilterParams) (bool, string, error)
type RetrievalDealFilter func(ctx context.Context, deal legacyretrievaltypes.ProviderDealState) (bool, string, error)

func CliStorageDealFilter(cmd string) StorageDealFilter {
	return func(ctx context.Context, deal DealFilterParams) (bool, string, error) {
		d := struct {
			types.DealParams
			SealingPipelineState sealingpipeline.Status
			FundsState           funds.Status
			StorageState         storagespace.Status
			NetworkVersion       network.Version
			DealType             string
			FormatVersion        string
			Agent                string
		}{
			DealParams:           deal.DealParams,
			SealingPipelineState: deal.SealingPipelineState,
			FundsState:           deal.FundsState,
			StorageState:         deal.StorageState,
			NetworkVersion:       deal.NetworkVersion,
			DealType:             "storage",
			FormatVersion:        storageFilterVersion,
			Agent:                agent,
		}
		return runDealFilter(ctx, cmd, d)
	}
}

func CliRetrievalDealFilter(cmd string) RetrievalDealFilter {
	return func(ctx context.Context, deal legacyretrievaltypes.ProviderDealState) (bool, string, error) {
		d := struct {
			legacyretrievaltypes.ProviderDealState
			DealType      string
			FormatVersion string
			Agent         string
		}{
			ProviderDealState: deal,
			DealType:          "retrieval",
			FormatVersion:     retrievalFilterVersion,
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
