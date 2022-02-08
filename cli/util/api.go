package cliutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/api/client"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/lotus/node/repo"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

const (
	metadataTraceContext = "traceContext"
)

var log = logging.Logger("cli")

// flagsForAPI returns flags passed on the command line with the listen address
// of the API server (only used by the tests), in the order of precedence they
// should be applied for the requested kind of node.
func flagsForAPI(t lotus_repo.RepoType) []string {
	switch t.Type() {
	case "Boost":
		return []string{"boost-api-url"}
	default:
		panic(fmt.Sprintf("Unknown repo type: %v", t))
	}
}

func flagsForRepo(t lotus_repo.RepoType) []string {
	switch t.Type() {
	case "Boost":
		return []string{"boost-repo"}
	default:
		panic(fmt.Sprintf("Unknown repo type: %v", t))
	}
}

// EnvsForAPIInfos returns the environment variables to use in order of precedence
// to determine the API endpoint of the specified node type.
//
// It returns the current variables and deprecated ones separately, so that
// the user can log a warning when deprecated ones are found to be in use.
func EnvsForAPIInfos(t lotus_repo.RepoType) (primary string, fallbacks []string, deprecated []string) {
	switch t.Type() {
	case "Boost":
		return "BOOST_API_INFO", []string{"BOOST_API_INFO"}, nil
	default:
		panic(fmt.Sprintf("Unknown repo type: %v", t))
	}
}

// GetAPIInfo returns the API endpoint to use for the specified kind of repo.
//
// The order of precedence is as follows:
//
//  1. *-api-url command line flags.
//  2. *_API_INFO environment variables
//  3. deprecated *_API_INFO environment variables
//  4. *-repo command line flags.
func GetAPIInfo(ctx *cli.Context, t lotus_repo.RepoType) (APIInfo, error) {
	// Check if there was a flag passed with the listen address of the API
	// server (only used by the tests)
	apiFlags := flagsForAPI(t)
	for _, f := range apiFlags {
		if !ctx.IsSet(f) {
			continue
		}
		strma := ctx.String(f)
		strma = strings.TrimSpace(strma)

		return APIInfo{Addr: strma}, nil
	}

	//
	// Note: it is not correct/intuitive to prefer environment variables over
	// CLI flags (repo flags below).
	//
	primaryEnv, fallbacksEnvs, deprecatedEnvs := EnvsForAPIInfos(t)
	env, ok := os.LookupEnv(primaryEnv)
	if ok {
		return ParseApiInfo(env), nil
	}

	for _, env := range deprecatedEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			log.Warnf("Using deprecated env(%s) value, please use env(%s) instead.", env, primaryEnv)
			return ParseApiInfo(env), nil
		}
	}

	repoFlags := flagsForRepo(t)
	for _, f := range repoFlags {
		// cannot use ctx.IsSet because it ignores default values
		path := ctx.String(f)
		if path == "" {
			continue
		}

		p, err := homedir.Expand(path)
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not expand home dir (%s): %w", f, err)
		}

		r, err := lotus_repo.NewFS(p)
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not open repo at path: %s; %w", p, err)
		}

		exists, err := r.Exists()
		if err != nil {
			return APIInfo{}, xerrors.Errorf("repo.Exists returned an error: %w", err)
		}

		if !exists {
			return APIInfo{}, errors.New("repo directory does not exist. Make sure your configuration is correct")
		}

		ma, err := r.APIEndpoint()
		if err != nil {
			return APIInfo{}, xerrors.Errorf("could not get api endpoint: %w", err)
		}

		token, err := r.APIToken()
		if err != nil {
			log.Warnf("Couldn't load CLI token, capabilities may be limited: %v", err)
		}

		return APIInfo{
			Addr:  ma.String(),
			Token: token,
		}, nil
	}

	for _, env := range fallbacksEnvs {
		env, ok := os.LookupEnv(env)
		if ok {
			return ParseApiInfo(env), nil
		}
	}

	return APIInfo{}, fmt.Errorf("could not determine API endpoint for node type: %v", t)
}

type GetBoostOptions struct {
	PreferHttp bool
}

type GetBoostOption func(*GetBoostOptions)

func BoostUseHttp(opts *GetBoostOptions) {
	opts.PreferHttp = true
}

func GetBoostAPI(ctx *cli.Context, opts ...GetBoostOption) (api.Boost, jsonrpc.ClientCloser, error) {
	var options GetBoostOptions
	for _, opt := range opts {
		opt(&options)
	}

	if tn, ok := ctx.App.Metadata["testnode-boost"]; ok {
		return tn.(api.Boost), func() {}, nil
	}

	addr, headers, err := GetRawAPI(ctx, node.BoostRepoType{}, "v0")
	if err != nil {
		return nil, nil, err
	}

	if options.PreferHttp {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, nil, xerrors.Errorf("parsing miner api URL: %w", err)
		}

		switch u.Scheme {
		case "ws":
			u.Scheme = "http"
		case "wss":
			u.Scheme = "https"
		}

		addr = u.String()
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintln(ctx.App.Writer, "using miner API v0 endpoint:", addr)
	}

	return client.NewBoostRPCV0(ctx.Context, addr, headers)
}

func GetRawAPI(ctx *cli.Context, t repo.RepoType, version string) (string, http.Header, error) {
	ainfo, err := GetAPIInfo(ctx, t)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get API info for %s: %w", t, err)
	}

	addr, err := ainfo.DialArgs(version)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get DialArgs: %w", err)
	}

	if IsVeryVerbose {
		_, _ = fmt.Fprintf(ctx.App.Writer, "using raw API %s endpoint: %s\n", version, addr)
	}

	return addr, ainfo.AuthHeader(), nil
}

func DaemonContext(cctx *cli.Context) context.Context {
	if mtCtx, ok := cctx.App.Metadata[metadataTraceContext]; ok {
		return mtCtx.(context.Context)
	}

	return context.Background()
}

// ReqContext returns context for cli execution. Calling it for the first time
// installs SIGTERM handler that will close returned context.
// Not safe for concurrent execution.
func ReqContext(cctx *cli.Context) context.Context {
	tCtx := DaemonContext(cctx)

	ctx, done := context.WithCancel(tCtx)
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		done()
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return ctx
}
