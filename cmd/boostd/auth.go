package main

import (
	"errors"
	"fmt"

	"github.com/filecoin-project/boost/node/repo"
	"github.com/urfave/cli/v2"

	bcli "github.com/filecoin-project/boost/cli"
	boostcliutil "github.com/filecoin-project/boost/cli/util"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/lotus/api"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

var authCmd = &cli.Command{
	Name:  "auth",
	Usage: "Manage RPC permissions",
	Subcommands: []*cli.Command{
		AuthCreateAdminToken,
		AuthApiInfoToken,
	},
}

var AuthCreateAdminToken = &cli.Command{
	Name:  "create-token",
	Usage: "Create token",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "perm",
			Usage: "permission to assign to the token, one of: read, write, sign, admin",
		},
	},

	Action: func(cctx *cli.Context) error {
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)

		if !cctx.IsSet("perm") {
			return errors.New("--perm flag not set")
		}

		perm := cctx.String("perm")
		idx := 0
		for i, p := range api.AllPermissions {
			if auth.Permission(perm) == p {
				idx = i + 1
			}
		}

		if idx == 0 {
			return fmt.Errorf("--perm flag has to be one of: %s", api.AllPermissions)
		}

		// slice on [:idx] so for example: 'sign' gives you [read, write, sign]
		token, err := napi.AuthNew(ctx, api.AllPermissions[:idx])
		if err != nil {
			return err
		}

		// TODO: Log in audit log when it is implemented

		fmt.Println(string(token))
		return nil
	},
}

var AuthApiInfoToken = &cli.Command{
	Name:  "api-info",
	Usage: "Get token with API info required to connect to this node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "perm",
			Usage: "permission to assign to the token, one of: read, write, sign, admin",
		},
	},

	Action: func(cctx *cli.Context) error {
		napi, closer, err := bcli.GetBoostAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := bcli.ReqContext(cctx)

		if !cctx.IsSet("perm") {
			return errors.New("--perm flag not set, use with one of: read, write, sign, admin")
		}

		perm := cctx.String("perm")
		idx := 0
		for i, p := range api.AllPermissions {
			if auth.Permission(perm) == p {
				idx = i + 1
			}
		}

		if idx == 0 {
			return fmt.Errorf("--perm flag has to be one of: %s", api.AllPermissions)
		}

		// slice on [:idx] so for example: 'sign' gives you [read, write, sign]
		token, err := napi.AuthNew(ctx, api.AllPermissions[:idx])
		if err != nil {
			return err
		}

		ainfo, err := cliutil.GetAPIInfo(cctx, repo.Boost)
		if err != nil {
			return fmt.Errorf("could not get API info for %s: %w", repo.Boost, err)
		}

		currentEnv, _, _ := boostcliutil.EnvsForAPIInfos(repo.Boost)
		fmt.Printf("%s=%s:%s\n", currentEnv, string(token), ainfo.Addr)
		return nil
	},
}
