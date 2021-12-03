package gql

import "net/http"

// Sets CORS headers to allow all
type corsHandler struct {
	sub http.Handler
}

func (h *corsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE, PUT")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	if r.Method == "OPTIONS" {
		_, _ = w.Write([]byte("OK"))
		return
	}

	h.sub.ServeHTTP(w, r)
}
