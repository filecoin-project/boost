package gql

import (
	"context"
	"fmt"
	"net/http"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const downloadBlockPath = "/download/block/"

type BlockGetter interface {
	Get(context.Context, cid.Cid) (blocks.Block, error)
}

// Serve downloads of raw blocks (for debugging purposes)
func serveDownload(ctx context.Context, mux *http.ServeMux, bstore BlockGetter) {
	mux.HandleFunc(downloadBlockPath, func(writer http.ResponseWriter, request *http.Request) {
		if len(request.URL.Path) <= len(downloadBlockPath) {
			writeError(writer, fmt.Sprint("url path too short: "+request.URL.Path))
			return
		}

		cidstr := request.URL.Path[len(downloadBlockPath):]
		c, err := cid.Parse(cidstr)
		if err != nil {
			writeError(writer, fmt.Sprint("parsing payload cid "+cidstr+": "+err.Error()))
			return
		}

		blk, err := bstore.Get(ctx, c)
		if err != nil {
			writeError(writer, fmt.Sprint("getting block "+cidstr+": "+err.Error()))
			return
		}

		writer.Header().Set("Content-Type", "application/vnd.ipld.raw")
		_, _ = writer.Write(blk.RawData())
	})
}

func writeError(writer http.ResponseWriter, s string) {
	_, _ = writer.Write([]byte(s))
	writer.WriteHeader(http.StatusBadRequest)
}
