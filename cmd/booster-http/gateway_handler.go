package main

import (
	"fmt"
	"github.com/ipfs/go-libipfs/gateway"
	"mime"
	"net/http"
	"strings"
)

type gatewayHandler struct {
	gwh              http.Handler
	supportedFormats map[string]struct{}
}

func newGatewayHandler(gw *BlocksGateway, supportedFormats []string) http.Handler {
	headers := map[string][]string{}
	gateway.AddAccessControlHeaders(headers)

	fmtsMap := make(map[string]struct{}, len(supportedFormats))
	for _, f := range supportedFormats {
		fmtsMap[f] = struct{}{}
	}

	return &gatewayHandler{
		gwh:              gateway.NewHandler(gateway.Config{Headers: headers}, gw),
		supportedFormats: fmtsMap,
	}
}

func (h *gatewayHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	responseFormat, _, err := customResponseFormat(r)
	if err != nil {
		webError(w, fmt.Errorf("error while processing the Accept header: %w", err), http.StatusBadRequest)
		return
	}

	if _, ok := h.supportedFormats[responseFormat]; !ok {
		if responseFormat == "" {
			responseFormat = "unixfs"
		}
		webError(w, fmt.Errorf("unsupported response format: %s", responseFormat), http.StatusBadRequest)
		return
	}

	h.gwh.ServeHTTP(w, r)
}

func webError(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}

// Unfortunately this function is not exported from go-libipfs so we need to copy it here.
// return explicit response format if specified in request as query parameter or via Accept HTTP header
func customResponseFormat(r *http.Request) (mediaType string, params map[string]string, err error) {
	if formatParam := r.URL.Query().Get("format"); formatParam != "" {
		// translate query param to a content type
		switch formatParam {
		case "raw":
			return "application/vnd.ipld.raw", nil, nil
		case "car":
			return "application/vnd.ipld.car", nil, nil
		case "tar":
			return "application/x-tar", nil, nil
		case "json":
			return "application/json", nil, nil
		case "cbor":
			return "application/cbor", nil, nil
		case "dag-json":
			return "application/vnd.ipld.dag-json", nil, nil
		case "dag-cbor":
			return "application/vnd.ipld.dag-cbor", nil, nil
		case "ipns-record":
			return "application/vnd.ipfs.ipns-record", nil, nil
		}
	}
	// Browsers and other user agents will send Accept header with generic types like:
	// Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
	// We only care about explicit, vendor-specific content-types and respond to the first match (in order).
	// TODO: make this RFC compliant and respect weights (eg. return CAR for Accept:application/vnd.ipld.dag-json;q=0.1,application/vnd.ipld.car;q=0.2)
	for _, header := range r.Header.Values("Accept") {
		for _, value := range strings.Split(header, ",") {
			accept := strings.TrimSpace(value)
			// respond to the very first matching content type
			if strings.HasPrefix(accept, "application/vnd.ipld") ||
				strings.HasPrefix(accept, "application/x-tar") ||
				strings.HasPrefix(accept, "application/json") ||
				strings.HasPrefix(accept, "application/cbor") ||
				strings.HasPrefix(accept, "application/vnd.ipfs") {
				mediatype, params, err := mime.ParseMediaType(accept)
				if err != nil {
					return "", nil, err
				}
				return mediatype, params, nil
			}
		}
	}
	// If none of special-cased content types is found, return empty string
	// to indicate default, implicit UnixFS response should be prepared
	return "", nil, nil
}
