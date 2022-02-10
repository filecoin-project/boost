package testutil

import "io/ioutil"

type CountWriter struct {
	Total int
}

func (cw *CountWriter) Write(p []byte) (int, error) {
	n, err := ioutil.Discard.Write(p)
	cw.Total += n
	return n, err
}
