package gql

import (
	"fmt"
	"net/http"
	"os"
)

const DummyDealsDir = "/tmp/dummy"
const DummyDealsPrefix = "dummy"

var DummyDealsBase = fmt.Sprintf("http://localhost:%d/"+DummyDealsPrefix, httpPort)

func serveDummyDeals() error {
	// Serve dummy-deals
	dpath := "/" + DummyDealsPrefix + "/"
	if err := os.MkdirAll(DummyDealsDir, 0755); err != nil {
		return fmt.Errorf("failed to mk directory %s for dummy deals: %w", DummyDealsDir, err)
	}
	http.Handle(dpath, http.StripPrefix(dpath, http.FileServer(http.Dir(DummyDealsDir))))
	return nil
}
