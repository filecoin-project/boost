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

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal"
	nilrouting "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/bitswap"
	lotus_blockstore "github.com/filecoin-project/lotus/blockstore"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/boxo/bitswap/client"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipldlegacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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

		host, err := createClientHost(privKey)
		if err != nil {
			return err
		}

		// Create a bitswap client
		nilRouter := nilrouting.Null{}
		net := bsnetwork.NewFromIpfsHost(host, nilRouter)
		bs, err := blockstore.OpenReadWrite(outputCarPath, []cid.Cid{rootCid}, blockstore.UseWholeCIDs(true))
		if err != nil {
			return fmt.Errorf("creating blockstore at %s: %w", outputCarPath, err)
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		idbs := lotus_blockstore.WrapIDStore(bs)
		brn := &blockReceiver{bs: idbs, ctx: ctx, cancel: cancel}
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

func createClientHost(privKey crypto.PrivKey) (host.Host, error) {
	return libp2p.New(
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.Identity(privKey),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)
}

func getBlocks(ctx context.Context, bsClient *client.Client, c cid.Cid, throttle chan struct{}) (uint64, uint64, error) {
	var size uint64
	var links []cid.Cid
	if c.Prefix().MhType == mh.IDENTITY {
		var err error
		size, links, err = getIDBlock(c)
		if err != nil {
			return 0, 0, err
		}
	} else {
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

		size = uint64(len(blk.RawData()))
		log.Debugw("receive", "cid", c, "size", size, "duration", time.Since(start).String())

		// Read the links from the block to child nodes in the DAG
		ipldDecoder := ipldlegacy.NewDecoder()
		nd, err := ipldDecoder.DecodeNode(ctx, blk)
		if err != nil {
			return 0, 0, fmt.Errorf("decoding node %s: %w", c, err)
		}

		ndLinks := nd.Links()
		for _, l := range ndLinks {
			links = append(links, l.Cid)
		}
	}

	var count = uint64(1)
	var eg errgroup.Group
	for _, link := range links {
		link := link
		// Launch a go routine to fetch the blocks underneath each link
		eg.Go(func() error {
			cnt, sz, err := getBlocks(ctx, bsClient, link, throttle)
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

func getIDBlock(c cid.Cid) (uint64, []cid.Cid, error) {
	dmh, err := mh.Decode(c.Hash())
	if err != nil {
		return 0, nil, err
	}

	if dmh.Code != mh.IDENTITY {
		return 0, nil, fmt.Errorf("bad cid: multihash type identity but decoded mh is not identity")
	}

	decoder, err := cidlink.DefaultLinkSystem().DecoderChooser(cidlink.Link{Cid: c})
	if err != nil {
		return 0, nil, fmt.Errorf("choosing decoder for identity CID %s: %w", c, err)
	}
	node, err := ipld.Decode(dmh.Digest, decoder)
	if err != nil {
		return 0, nil, fmt.Errorf("decoding identity CID %s: %w", c, err)
	}
	links, err := traversal.SelectLinks(node)
	if err != nil {
		return 0, nil, fmt.Errorf("collecting links from identity CID %s: %w", c, err)
	}
	// convert from Link to Cid
	resultCids := make([]cid.Cid, 0)
	for _, link_ := range links {
		resultCids = append(resultCids, link_.(cidlink.Link).Cid)
	}
	return uint64(len(dmh.Digest)), resultCids, nil
}

type blockReceiver struct {
	bs     lotus_blockstore.Blockstore
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
