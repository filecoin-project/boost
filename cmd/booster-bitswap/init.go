package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

func configureRepo(cfgDir string, createIfNotExist bool) (peer.ID, crypto.PrivKey, error) {
	if cfgDir == "" {
		return "", nil, fmt.Errorf("%s is a required flag", FlagRepo.Name)
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

func setupHost(cfgDir string, port int) (host.Host, error) {
	_, peerKey, err := configureRepo(cfgDir, false)
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
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.Identity(peerKey),
		libp2p.ResourceManager(&network.NullResourceManager{}),
	)
}

func loadPeerKey(cfgDir string, createIfNotExists bool) (crypto.PrivKey, error) {
	keyPath := getKeyPath(cfgDir)
	keyFile, err := os.ReadFile(keyPath)
	if err == nil {
		return crypto.UnmarshalPrivateKey(keyFile)
	}

	if !os.IsNotExist(err) {
		return nil, err
	}
	if !createIfNotExists {
		msg := keyPath + " not found: "
		msg += "booster-bitswap has not been initialized. Run the booster-bitswap init command"
		return nil, fmt.Errorf(msg)
	}
	log.Infof("Generating new peer key...")

	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	data, err := crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfgDir, 0744); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, data, 0600); err != nil {
		return nil, err
	}

	return key, nil
}

func getKeyPath(cfgDir string) string {
	return filepath.Join(cfgDir, "libp2p.key")
}

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Init booster-bitswap config",
	Before: before,
	Flags:  []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}

		peerID, _, err := configureRepo(repoDir, true)
		fmt.Println("Initialized booster-bitswap with libp2p peer ID " + peerID.String())
		fmt.Println("Key file: " + getKeyPath(repoDir))
		return err
	},
}
