package util

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const Libp2pScheme = "libp2p"

type TransportUrl struct {
	Scheme    string
	Url       string
	PeerID    peer.ID
	Multiaddr multiaddr.Multiaddr
}

func ParseUrl(urlStr string) (*TransportUrl, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("parsing url '%s': %w", urlStr, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("parsing url '%s': could not parse scheme", urlStr)
	}
	if u.Scheme == Libp2pScheme {
		return parseLibp2pUrl(urlStr)
	}
	return &TransportUrl{Scheme: u.Scheme, Url: urlStr}, nil
}

func parseLibp2pUrl(urlStr string) (*TransportUrl, error) {
	// Remove libp2p prefix
	prefix := Libp2pScheme + "://"
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

	return &TransportUrl{
		Scheme:    Libp2pScheme,
		Url:       Libp2pScheme + "://" + addrInfo.ID.String(),
		PeerID:    addrInfo.ID,
		Multiaddr: addrInfo.Addrs[0],
	}, nil
}
