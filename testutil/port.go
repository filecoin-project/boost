package testutil

import "net"

func FreePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = l.Close()
	}()
	return l.Addr().(*net.TCPAddr).Port, nil
}
