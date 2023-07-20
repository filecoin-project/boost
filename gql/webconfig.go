package gql

import (
	"encoding/json"
	"net/http"
)

type webConfigIpni struct {
	IndexerHost string
}

type webConfig struct {
	Ipni webConfigIpni
}

type webConfigServer struct {
	cfg webConfig
}

func (s *webConfigServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	bz, err := json.Marshal(&s.cfg)
	if err != nil {
		_, _ = writer.Write([]byte("error serving web config: " + err.Error()))
		writer.WriteHeader(500)
		return
	}

	_, _ = writer.Write(bz)
}
