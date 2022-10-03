package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/ipld/go-car/v2/blockstore"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/boost/tracing"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-bitswap/client"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/pkg/profile"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

var fetchCmd = &cli.Command{
	Name:        "fetch",
	Usage:       "fetch <multiaddr> <root cid> <output car path>",
	Description: "Fetch all blocks in the DAG under the given root cid from the bitswap node at multiaddr",
	Before:      before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6070",
		},
		&cli.IntFlag{
			Name:  "concurrency",
			Usage: "concurrent request limit - 0 means unlimited",
			Value: 10,
		},
		&cli.BoolFlag{
			Name:  "tracing",
			Usage: "enables tracing of booster-bitswap calls",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "tracing-endpoint",
			Usage: "the endpoint for the tracing exporter",
			Value: "http://tempo:14268/api/traces",
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 3 {
			return fmt.Errorf("usage: fetch <multiaddr> <root cid> <output car path>")
		}

		addrInfoStr := cctx.Args().Get(0)
		serverAddrInfo, err := peer.AddrInfoFromString(addrInfoStr)
		if err != nil {
			return fmt.Errorf("parsing server multiaddr %s: %w", addrInfoStr, err)
		}

		rootCidStr := cctx.Args().Get(1)
		rootCid, err := cid.Parse(rootCidStr)
		if err != nil {
			return fmt.Errorf("parsing cid %s: %w", rootCidStr, err)
		}

		outputCarPath := cctx.Args().Get(2)

		defer profile.Start(profile.TraceProfile, profile.ProfilePath(".")).Stop()

		if cctx.Bool("pprof") {
			go func() {
				err := http.ListenAndServe("localhost:6065", nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}

		ctx := lcli.ReqContext(cctx)

		// Instantiate the tracer and exporter
		if cctx.Bool("tracing") {
			tracingStopper, err := tracing.New("booster-bsclient", cctx.String("tracing-endpoint"))
			if err != nil {
				return fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			log.Info("Tracing exporter enabled")

			defer func() {
				_ = tracingStopper(ctx)
			}()
		}

		// setup libp2p host
		log.Infow("generating libp2p key")
		privKey, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
		if err != nil {
			return err
		}

		host, err := libp2p.New(
			libp2p.Transport(tcp.NewTCPTransport),
			libp2p.Transport(quic.NewTransport),
			libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
			libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
			libp2p.Identity(privKey),
			libp2p.ResourceManager(network.NullResourceManager),
		)
		if err != nil {
			return err
		}

		// Create a bitswap client
		nilRouter, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
		if err != nil {
			return err
		}
		net := bsnetwork.NewFromIpfsHost(host, nilRouter)
		bs, err := blockstore.OpenReadWrite(outputCarPath, []cid.Cid{rootCid}, blockstore.UseWholeCIDs(true))
		if err != nil {
			return fmt.Errorf("creating blockstore at %s: %w", outputCarPath, err)
		}

		bsClient := client.New(ctx, net, bs)
		net.Start(bsClient)

		log.Infow("connecting to server", "server", serverAddrInfo.String())
		err = host.Connect(ctx, *serverAddrInfo)
		if err != nil {
			return fmt.Errorf("connecting to %s: %w", serverAddrInfo, err)
		}

		var throttle chan struct{}
		concurrency := cctx.Int("concurrency")
		if concurrency > 0 {
			throttle = make(chan struct{}, concurrency)
		}

		log.Infow("fetch", "cid", rootCid, "concurrency", concurrency)
		start := time.Now()
		count, size, err := getBlocks(ctx, bsClient, rootCid, throttle)
		if err != nil {
			return fmt.Errorf("getting blocks: %w", err)
		}

		log.Infow("complete", "count", count, "size", size, "duration", time.Since(start))
		return nil
	},
}

func getBlocks(ctx context.Context, bsClient *client.Client, c cid.Cid, throttle chan struct{}) (uint64, uint64, error) {
	if throttle != nil {
		throttle <- struct{}{}
	}
	// Get the block
	start := time.Now()
	blk, err := bsClient.GetBlock(ctx, c)
	if throttle != nil {
		<-throttle
	}
	if err != nil {
		return 0, 0, err
	}

	var size = uint64(len(blk.RawData()))
	log.Debugw("receive", "cid", c, "size", size, "duration", time.Since(start))

	// Read the links from the block to child nodes in the DAG
	var count = uint64(1)
	nd, err := ipldlegacy.DecodeNode(ctx, blk)
	if err != nil {
		return 0, 0, fmt.Errorf("decoding node %s: %w", c, err)
	}

	var eg errgroup.Group
	lnks := nd.Links()
	for _, l := range lnks {
		l := l
		// Launch a go routine to fetch the blocks underneath each link
		eg.Go(func() error {
			cnt, sz, err := getBlocks(ctx, bsClient, l.Cid, throttle)
			if err != nil {
				return err
			}
			atomic.AddUint64(&count, cnt)
			atomic.AddUint64(&size, sz)
			return nil
		})
	}
	return count, size, eg.Wait()
}
