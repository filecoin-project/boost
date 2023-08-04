package clientutil

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
)

func NewTestStore(ctx context.Context) *client.Store {
	bdsvc, err := svc.NewLevelDB("")
	if err != nil {
		panic(err)
	}
	ln, err := bdsvc.Start(ctx, "localhost:0")
	if err != nil {
		panic(err)
	}

	cl := client.NewStore()
	err = cl.Dial(ctx, fmt.Sprintf("ws://%s", ln.String()))
	if err != nil {
		panic(err)
	}

	return cl
}
