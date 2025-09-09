package util

import (
	"fmt"
	"net"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
)

func ToHttpMultiaddr(hostname string, port int) (ma.Multiaddr, error) {
	if hostname == "" {
		return nil, fmt.Errorf("hostname is empty")
	}

	var saddr string
	if n := net.ParseIP(hostname); n != nil {
		ipVersion := "ip4"
		if strings.Contains(hostname, ":") {
			ipVersion = "ip6"
		}
		saddr = fmt.Sprintf("/%s/%s/tcp/%d/http", ipVersion, hostname, port)
	} else {
		saddr = fmt.Sprintf("/dns/%s/tcp/%d/http", hostname, port)
	}
	return ma.NewMultiaddr(saddr)
}
