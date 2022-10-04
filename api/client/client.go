package client

import (
	"context"
	"net/http"
	"net/url"
	"path"

	"github.com/dirkmc/go-jsonrpc"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/lib/rpcenc"
)

func getPushUrl(addr string) (string, error) {
	pushUrl, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushUrl.Scheme {
	case "ws":
		pushUrl.Scheme = "http"
	case "wss":
		pushUrl.Scheme = "https"
	}
	///rpc/v0 -> /rpc/streams/v0/push

	pushUrl.Path = path.Join(pushUrl.Path, "../streams/v0/push")
	return pushUrl.String(), nil
}

// NewBoostRPCV0 creates a new http jsonrpc client for miner
func NewBoostRPCV0(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Boost, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.BoostStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Filecoin",
		api.GetInternalStructs(&res), requestHeader,
		append([]jsonrpc.Option{
			rpcenc.ReaderParamEncoder(pushUrl),
		}, opts...)...)

	return &res, closer, err
}
