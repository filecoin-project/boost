package main

import (

	"net/http"

	"github.com/ipfs/boxo/gateway"
)

type gatewayHandler struct {
	gwh              http.Handler
}

func newGatewayHandler(gw *gateway.BlocksBackend, serveGateway string) http.Handler {
	headers := map[string][]string{}
	var deserializedResponse bool
	gateway.AddAccessControlHeaders(headers)

	if serveGateway == "none" {
		return &gatewayHandler{}
	}
	if serveGateway == "all" {
		deserializedResponse = true
	} else if serveGateway == "verifiable" {
		deserializedResponse = false
	}
	return &gatewayHandler{
		gwh: gateway.NewHandler(gateway.Config{
			Headers:               headers,
			DeserializedResponses: deserializedResponse,
		}, gw),
	}
}

func (h *gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.gwh.ServeHTTP(w, r)
}
