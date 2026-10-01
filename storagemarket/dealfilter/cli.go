package dealfilter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

const agent = "boost"

// storageJsonVersion is the version of the storage deal JSON document handed to an external filter;
// 2.3.0 adds NetworkVersion, which a filter sorting FIL+ traffic by VerifiedDeal needs at nv29.
const storageJsonVersion = "2.3.0"

// retrievalJsonVersion is the version of the retrieval deal JSON document; it does not move with
// the storage one, which would tell filters to expect a shape that never arrived.
const retrievalJsonVersion = "2.2.0"

type StorageDealFilter func(ctx context.Context, deal DealFilterParams) (bool, string, error)
type RetrievalDealFilter func(ctx context.Context, deal legacyretrievaltypes.ProviderDealState) (bool, string, error)

func CliStorageDealFilter(cmd string) StorageDealFilter {
	return func(ctx context.Context, deal DealFilterParams) (bool, string, error) {
		// A zero is an unresolved version, not nv0: it reads to a filter as a very old chain, so refuse
		// rather than send a script down the pre-nv29 path.
		if deal.NetworkVersion == 0 {
			return false, "server error: deal filter network version missing",
				errors.New("storage deal filter params carry no network version")
		}

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
			FormatVersion:        storageJsonVersion,
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
			FormatVersion:     retrievalJsonVersion,
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
