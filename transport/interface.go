package transport

import (
	"context"
	"encoding/json"
	"fmt"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/transport/types"
)

type Transport interface {
	Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th Handler, err error)
}

type Handler interface {
	Sub() chan types.TransportEvent
	Close()
}

func TransferParamsAsJson(transfer smtypes.Transfer) (string, error) {
	if transfer.Type != "http" {
		return "", fmt.Errorf("cannot parse params for unrecognized transfer type '%s'", transfer.Type)
	}

	// de-serialize transport opaque token
	tInfo := &types.HttpRequest{}
	if err := json.Unmarshal(transfer.Params, tInfo); err != nil {
		return "", fmt.Errorf("failed to de-serialize transport params bytes '%s': %w", string(transfer.Params), err)
	}

	// Just extract the URL, not the headers, because the headers may contain
	// sensitive information that we don't want to end up in a log file
	// somewhere (eg Authorization header)
	bz, err := json.Marshal(map[string]string{
		"URL": tInfo.URL,
	})
	if err != nil {
		return "", fmt.Errorf("marshalling transfer params json: %w", err)
	}
	return string(bz), nil
}
