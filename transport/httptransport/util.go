package httptransport

import (
	"fmt"
	"net/url"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
	manet "github.com/multiformats/go-multiaddr/net"
)

type httpError struct {
	error
	code int
}

type transportUrl struct {
	Scheme    string
	URL       string
	Host      string
	PeerID    peer.ID
	Multiaddr multiaddr.Multiaddr
}

func parseUrl(urlStr string) (*transportUrl, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme == libp2pScheme {
		return parseLibp2pUrl(urlStr)
	}
	return &transportUrl{Scheme: u.Scheme, URL: urlStr}, nil
}

func parseLibp2pUrl(urlStr string) (*transportUrl, error) {
	// Remove libp2p prefix
	prefix := libp2pScheme + "://"
	mastr := urlStr[len(prefix):]
	addr, err := multiaddr.NewMultiaddr(mastr)
	if err != nil {
		return nil, fmt.Errorf("parsing '%s' as multiaddr: %w", mastr, err)
	}

	// Must have a P2P component so we can get the peer ID
	httpAddr, last := multiaddr.SplitLast(addr)
	if last.Protocol().Code != multiaddr.P_P2P {
		return nil, fmt.Errorf("missing peerID in url '%s'", urlStr)
	}
	peerID := last.Value()

	// If it's a DNS address like "/dnsaddr/bootstrap.libp2p.io"
	if mafmt.DNS.Matches(httpAddr) {
		// Get the host part of the address
		host := ""
		multiaddr.ForEach(httpAddr, func(c multiaddr.Component) bool {
			host = c.Value()
			return false
		})

		return &transportUrl{
			Scheme:    libp2pScheme,
			Host:      host,
			URL:       libp2pScheme + "://" + peerID,
			PeerID:    peer.ID(peerID),
			Multiaddr: httpAddr,
		}, nil
	}

	// Convert multi-address to HTTP url
	netaddr, err := manet.ToNetAddr(httpAddr)
	if err != nil {
		return nil, fmt.Errorf("parsing '%s' as HTTP url: %w", httpAddr.String(), err)
	}

	return &transportUrl{
		Scheme:    libp2pScheme,
		Host:      netaddr.String(),
		URL:       libp2pScheme + "://" + peerID,
		PeerID:    peer.ID(peerID),
		Multiaddr: httpAddr,
	}, nil
}
