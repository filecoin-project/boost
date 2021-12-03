package gql

import (
	"fmt"
	"net/http"
	"os"

	"github.com/filecoin-project/boost/testutil"
)

const DummyDealsDir = "/tmp/dummy"
const DummyDealsPrefix = "dummy"

var DummyDealsBase = fmt.Sprintf("http://localhost:%d/"+DummyDealsPrefix, httpPort)

func serveDummyDeals() error {
	dpath := "/" + DummyDealsPrefix + "/"
	if err := os.MkdirAll(DummyDealsDir, 0755); err != nil {
		return fmt.Errorf("failed to mk directory %s for dummy deals: %w", DummyDealsDir, err)
	}
	fileSystem := &testutil.SlowFileOpener{Dir: DummyDealsDir}
	http.Handle(dpath, http.StripPrefix(dpath, http.FileServer(fileSystem)))
	return nil
}
