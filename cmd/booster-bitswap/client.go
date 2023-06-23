package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/bitswap"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/boxo/bitswap/client"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	nilrouting "github.com/ipfs/boxo/routing/none"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
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
			Usage: "run pprof web server on localhost:6071",
		},
		&cli.IntFlag{
			Name:  "concurrency",
			Usage: "concurrent request limit - 0 means unlimited",
			Value: 10,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("pprof") {
			go func() {
				err := http.ListenAndServe("localhost:6071", nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}

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

		ctx := lcli.ReqContext(cctx)

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
			libp2p.ResourceManager(&network.NullResourceManager{}),
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

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		brn := &blockReceiver{bs: bs, ctx: ctx, cancel: cancel}
		bsClient := client.New(ctx, net, bs, client.WithBlockReceivedNotifier(brn))
		defer bsClient.Close()
		net.Start(bsClient)

		// Connect to host
		connectStart := time.Now()
		log.Infow("connecting to server", "server", serverAddrInfo.String())
		err = host.Connect(ctx, *serverAddrInfo)
		if err != nil {
			return fmt.Errorf("connecting to %s: %w", serverAddrInfo, err)
		}
		log.Debugw("connected to server", "duration", time.Since(connectStart).String())

		// Check host's libp2p protocols
		protos, err := host.Peerstore().GetProtocols(serverAddrInfo.ID)
		if err != nil {
			return fmt.Errorf("getting protocols from peer store for %s: %w", serverAddrInfo.ID, err)
		}
		sort.Slice(protos, func(i, j int) bool {
			return protos[i] < protos[j]
		})
		log.Debugw("host libp2p protocols", "protocols", protos)
		p, err := host.Peerstore().FirstSupportedProtocol(serverAddrInfo.ID, bitswap.Protocols...)
		if err != nil {
			return fmt.Errorf("getting first supported protocol from peer store for %s: %w", serverAddrInfo.ID, err)
		}
		if p == "" {
			return fmt.Errorf("host %s does not support any know bitswap protocols: %s", serverAddrInfo.ID, bitswap.ProtocolStrings)
		}

		var throttle chan struct{}
		concurrency := cctx.Int("concurrency")
		if concurrency > 0 {
			throttle = make(chan struct{}, concurrency)
		}

		// Fetch all blocks under the root cid
		log.Infow("fetch", "cid", rootCid, "concurrency", concurrency)
		start := time.Now()
		count, size, err := getBlocks(ctx, bsClient, rootCid, throttle)
		if err != nil {
			return fmt.Errorf("getting blocks: %w", err)
		}

		log.Infow("fetch complete", "count", count, "size", size, "duration", time.Since(start).String())
		log.Debug("finalizing")
		finalizeStart := time.Now()
		defer func() { log.Infow("finalize complete", "duration", time.Since(finalizeStart).String()) }()
		return bs.Finalize()
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
	log.Debugw("receive", "cid", c, "size", size, "duration", time.Since(start).String())

	// Read the links from the block to child nodes in the DAG
	var count = uint64(1)
	ipldDecoder := ipldlegacy.NewDecoder()
	nd, err := ipldDecoder.DecodeNode(ctx, blk)
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

type blockReceiver struct {
	bs     *blockstore.ReadWrite
	ctx    context.Context
	cancel context.CancelFunc
}

func (b blockReceiver) ReceivedBlocks(id peer.ID, blks []blocks.Block) {
	err := b.bs.PutMany(b.ctx, blks)
	if err != nil {
		log.Errorw("failed to write blocks to blockstore: %s", err)
		b.cancel()
	}
}
