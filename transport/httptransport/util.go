package httptransport

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type httpError struct {
	error
	code int
}

type transportUrl struct {
	scheme    string
	url       string
	peerID    peer.ID
	multiaddr multiaddr.Multiaddr
}

func parseUrl(urlStr string) (*transportUrl, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("parsing url '%s': %w", urlStr, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("parsing url '%s': could not parse scheme", urlStr)
	}
	if u.Scheme == libp2pScheme {
		return parseLibp2pUrl(urlStr)
	}
	return &transportUrl{scheme: u.Scheme, url: urlStr}, nil
}

func parseLibp2pUrl(urlStr string) (*transportUrl, error) {
	// Remove libp2p prefix
	prefix := libp2pScheme + "://"
	if !strings.HasPrefix(urlStr, prefix) {
		return nil, fmt.Errorf("libp2p URL '%s' must start with prefix '%s'", urlStr, prefix)
	}

	// Convert to AddrInfo
	addrInfo, err := peer.AddrInfoFromString(urlStr[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("parsing address info from url '%s': %w", urlStr, err)
	}

	// There should be exactly one address
	if len(addrInfo.Addrs) != 1 {
		return nil, fmt.Errorf("expected only one address in url '%s'", urlStr)
	}

	return &transportUrl{
		scheme:    libp2pScheme,
		url:       libp2pScheme + "://" + addrInfo.ID.String(),
		peerID:    addrInfo.ID,
		multiaddr: addrInfo.Addrs[0],
	}, nil
}
