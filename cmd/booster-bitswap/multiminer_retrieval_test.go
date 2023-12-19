package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/bitswap"
	"github.com/filecoin-project/boost/itests/shared"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestMultiMinerBitswapRetrieval(t *testing.T) {
	shared.RunMultiminerRetrievalTest(t, func(ctx context.Context, t *testing.T, rt *shared.RetrievalTest) {
		miner1ApiInfo, err := rt.BoostAndMiner1.LotusMinerApiInfo()
		require.NoError(t, err)

		miner2ApiInfo, err := rt.BoostAndMiner2.LotusMinerApiInfo()
		require.NoError(t, err)

		fullNode2ApiInfo, err := rt.BoostAndMiner2.LotusFullNodeApiInfo()
		require.NoError(t, err)

		repoDir := t.TempDir()
		peerID, _, err := configureRepo(repoDir, true)
		require.NoError(t, err)

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()

		go func() {
			// Configure booster-bitswap to
			// - Get piece location information from the shared LID instance
			// - Get the piece data from either miner1 or miner2 (depending on the location info)
			apiInfo := []string{miner1ApiInfo, miner2ApiInfo}
			_ = runBoosterBitswap(runCtx, repoDir, apiInfo, fullNode2ApiInfo, "ws://localhost:8042")
		}()

		maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8888")
		require.NoError(t, err)

		privKey, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
		require.NoError(t, err)

		clientHost, err := createClientHost(privKey)
		require.NoError(t, err)

		t.Logf("waiting for server to come up")
		start := time.Now()
		require.Eventually(t, func() bool {
			err := connectToHost(ctx, clientHost, maddr, peerID)
			if err != nil {
				t.Logf("connecting to host: %s", err)
				return false
			}
			return true
		}, 30*time.Second, time.Second)
		t.Logf("booster-bitswap is up after %s", time.Since(start))

		outPath := path.Join(t.TempDir(), "out.dat")
		fetchAddr := maddr.String() + "/p2p/" + peerID.String()
		err = runBoosterBitswapFetch(ctx, fetchAddr, rt.RootCid.String(), outPath)
		require.NoError(t, err)

		t.Logf("retrieval is done, compare root cid %s to downloaded CAR root cid", rt.RootCid)
		r, err := carv2.OpenReader(outPath)
		require.NoError(t, err)

		roots, err := r.Roots()
		require.NoError(t, err)
		require.Len(t, roots, 1)
		require.Equal(t, rt.RootCid, roots[0])

		t.Logf("file retrieved successfully")
	})
}

func connectToHost(ctx context.Context, clientHost host.Host, maddr multiaddr.Multiaddr, pid peer.ID) error {
	// Connect to host
	err := clientHost.Connect(ctx, peer.AddrInfo{
		ID:    pid,
		Addrs: []multiaddr.Multiaddr{maddr},
	})
	if err != nil {
		return err
	}

	// Check host's libp2p protocols
	protos, err := clientHost.Peerstore().GetProtocols(pid)
	if err != nil {
		return fmt.Errorf("getting protocols from peer store for %s: %w", pid, err)
	}
	sort.Slice(protos, func(i, j int) bool {
		return protos[i] < protos[j]
	})
	fmt.Println("host libp2p protocols", "protocols", protos)
	p, err := clientHost.Peerstore().FirstSupportedProtocol(pid, bitswap.Protocols...)
	if err != nil {
		return fmt.Errorf("getting first supported protocol from peer store for %s: %w", pid, err)
	}
	if p == "" {
		return fmt.Errorf("host %s does not support any know bitswap protocols: %s", pid, bitswap.ProtocolStrings)
	}
	return nil
}

func runBoosterBitswap(ctx context.Context, repo string, minerApiInfo []string, fullNodeApiInfo string, lidApiInfo string) error {
	app.Setup()

	args := []string{"booster-bitswap",
		"--repo=" + repo,
		"run",
		"--api-fullnode=" + fullNodeApiInfo,
		"--api-lid=" + lidApiInfo,
		"--api-version-check=false",
		"--no-metrics",
	}
	for _, apiInfo := range minerApiInfo {
		args = append(args, "--api-storage="+apiInfo)
	}
	return app.RunContext(ctx, args)
}

func runBoosterBitswapFetch(ctx context.Context, multiaddr string, rootCid string, outputPath string) error {
	args := []string{"booster-bitswap", "fetch", multiaddr, rootCid, outputPath}
	return app.RunContext(ctx, args)
}
