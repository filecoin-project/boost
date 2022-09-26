package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/urfave/cli/v2"
)

func configureRepo(ctx context.Context, cfgDir string, createIfNotExist bool) (peer.ID, crypto.PrivKey, error) {
	if cfgDir == "" {
		return "", nil, fmt.Errorf("dataDir must be set")
	}

	if err := os.MkdirAll(cfgDir, 0744); err != nil {
		return "", nil, err
	}

	peerkey, err := loadPeerKey(cfgDir, createIfNotExist)
	if err != nil {
		return "", nil, err
	}

	selfPid, err := peer.IDFromPrivateKey(peerkey)
	if err != nil {
		return "", nil, err
	}

	return selfPid, peerkey, nil
}

func setupHost(ctx context.Context, cfgDir string, port int) (host.Host, error) {
	_, peerKey, err := configureRepo(ctx, cfgDir, false)
	if err != nil {
		return nil, err
	}
	return libp2p.New(
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", port),
		),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.Identity(peerKey),
		libp2p.ResourceManager(network.NullResourceManager),
	)
}

func loadPeerKey(cfgDir string, createIfNotExists bool) (crypto.PrivKey, error) {
	var peerkey crypto.PrivKey
	keyPath := filepath.Join(cfgDir, "libp2p.key")
	keyFile, err := os.ReadFile(keyPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if os.IsNotExist(err) && !createIfNotExists {
			return nil, err
		}
		log.Infof("Generating new peer key...")

		key, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		peerkey = key

		data, err := crypto.MarshalPrivateKey(key)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(keyPath, data, 0600); err != nil {
			return nil, err
		}
	} else {
		key, err := crypto.UnmarshalPrivateKey(keyFile)
		if err != nil {
			return nil, err
		}

		peerkey = key
	}

	if peerkey == nil {
		panic("sanity check: peer key is uninitialized")
	}

	return peerkey, nil
}

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Init booster-bitswap config",
	Before: before,
	Flags:  []cli.Flag{},
	Action: func(cctx *cli.Context) error {

		ctx := lcli.ReqContext(cctx)

		repoDir := cctx.String(FlagRepo.Name)

		peerID, _, err := configureRepo(ctx, repoDir, true)
		fmt.Println(peerID)
		return err
	},
}
