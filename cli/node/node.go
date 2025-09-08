package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/boost/lib/keystore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mitchellh/go-homedir"
)

type Node struct {
	Host   host.Host
	Wallet *wallet.LocalWallet
}

func Setup(cfgdir string) (*Node, error) {
	cfgdir, err := homedir.Expand(cfgdir)
	if err != nil {
		return nil, fmt.Errorf("getting homedir: %w", err)
	}

	_, err = os.Stat(cfgdir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("repo dir doesn't exist. run `boost init` first")
	}

	peerkey, err := loadOrInitPeerKey(keyPath(cfgdir))
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.Identity(peerkey),
	)
	if err != nil {
		return nil, err
	}

	wallet, err := setupWallet(walletPath(cfgdir))
	if err != nil {
		return nil, err
	}

	return &Node{
		Host:   h,
		Wallet: wallet,
	}, nil
}

func loadOrInitPeerKey(kf string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(kf)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		k, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}

		data, err := crypto.MarshalPrivateKey(k)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(kf, data, 0600); err != nil {
			return nil, err
		}

		return k, nil
	}
	return crypto.UnmarshalPrivateKey(data)
}

func setupWallet(dir string) (*wallet.LocalWallet, error) {
	kstore, err := keystore.OpenOrInitKeystore(dir)
	if err != nil {
		return nil, err
	}

	wallet, err := wallet.NewWallet(kstore)
	if err != nil {
		return nil, err
	}

	addrs, err := wallet.WalletList(context.TODO())
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		_, err := wallet.WalletNew(context.TODO(), types.KTBLS)
		if err != nil {
			return nil, err
		}
	}

	return wallet, nil
}

func keyPath(baseDir string) string {
	return filepath.Join(baseDir, "libp2p.key")
}

func walletPath(baseDir string) string {
	return filepath.Join(baseDir, "wallet")
}

func (n *Node) GetProvidedOrDefaultWallet(ctx context.Context, provided string) (address.Address, error) {
	var walletAddr address.Address
	if provided == "" {
		var err error
		walletAddr, err = n.Wallet.GetDefault()
		if err != nil {
			return address.Address{}, err
		}
	} else {
		w, err := address.NewFromString(provided)
		if err != nil {
			return address.Address{}, err
		}

		addrs, err := n.Wallet.WalletList(ctx)
		if err != nil {
			return address.Address{}, err
		}

		found := false
		for _, a := range addrs {
			if bytes.Equal(a.Bytes(), w.Bytes()) {
				walletAddr = w
				found = true
			}
		}

		if !found {
			return address.Address{}, fmt.Errorf("couldn't find wallet %s locally", provided)
		}
	}

	return walletAddr, nil
}
