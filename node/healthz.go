package node

import (
	"encoding/json"
	"net/http"
)

type healthzResponse struct {
	OK bool `json:"ok"`
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	res := healthzResponse{
		OK: true,
	}
	json.NewEncoder(w).Encode(res) //nolint:errcheck
}

func configz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	res := healthzResponse{
		OK: true,
	}
	json.NewEncoder(w).Encode(res) //nolint:errcheck
}
