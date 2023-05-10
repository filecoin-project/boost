package build

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"log"

	lotus_apitypes "github.com/filecoin-project/lotus/api/types"
)

//go:embed openrpc
var openrpcfs embed.FS

func mustReadGzippedOpenRPCDocument(data []byte) lotus_apitypes.OpenRPCDocument {
	zr, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		log.Fatal(err)
	}
	m := lotus_apitypes.OpenRPCDocument{}
	err = json.NewDecoder(zr).Decode(&m)
	if err != nil {
		log.Fatal(err)
	}
	err = zr.Close()
	if err != nil {
		log.Fatal(err)
	}
	return m
}

func OpenRPCDiscoverJSON_Boost() lotus_apitypes.OpenRPCDocument {
	data, err := openrpcfs.ReadFile("openrpc/boost.json.gz")
	if err != nil {
		panic(err)
	}
	return mustReadGzippedOpenRPCDocument(data)
}
