package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"go.opencensus.io/tag"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node/impl"

	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/metrics/proxy"
	"github.com/filecoin-project/lotus/lib/rpcenc"
)

var rpclog = logging.Logger("rpc")

// ServeRPC serves an HTTP handler over the supplied listen multiaddr.
//
// This function spawns a goroutine to run the server, and returns immediately.
// It returns the stop function to be called to terminate the endpoint.
//
// The supplied ID is used in tracing, by inserting a tag in the context.
func ServeRPC(h http.Handler, id string, addr multiaddr.Multiaddr) (StopFunc, error) {
	// Start listening to the addr; if invalid or occupied, we will fail early.
	lst, err := manet.Listen(addr)
	if err != nil {
		return nil, fmt.Errorf("could not listen: %w", err)
	}

	// Instantiate the server and start listening.
	srv := &http.Server{
		Handler: h,
		BaseContext: func(listener net.Listener) context.Context {
			ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, id))
			return ctx
		},
	}

	go func() {
		err = srv.Serve(manet.NetListener(lst))
		if err != http.ErrServerClosed {
			rpclog.Warnf("rpc server failed: %s", err)
		}
	}()

	return srv.Shutdown, err
}

// BoostHandler returns a boost service handler, to be mounted as-is on the server.
func BoostHandler(a api.Boost, permissioned bool) (http.Handler, error) {
	m := mux.NewRouter()

	mapi := proxy.MetricedBoostAPI(a)
	if permissioned {
		mapi = api.PermissionedBoostAPI(mapi)
	}

	readerHandler, readerServerOpt := rpcenc.ReaderParamDecoder()
	rpcServer := jsonrpc.NewServer(readerServerOpt)
	rpcServer.Register("Filecoin", mapi)
	rpcServer.AliasMethod("rpc.discover", "Filecoin.Discover")

	m.Handle("/rpc/v0", rpcServer)
	m.Handle("/rpc/streams/v0/push/{uuid}", readerHandler)
	m.PathPrefix("/remote").HandlerFunc(a.(*impl.BoostAPI).ServeRemote(permissioned))

	// debugging
	m.Handle("/metrics", metrics.Exporter("boost"))
	m.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

	if !permissioned {
		return m, nil
	}

	ah := &auth.Handler{
		Verify: a.AuthVerify,
		Next:   m.ServeHTTP,
	}
	return ah, nil
}
