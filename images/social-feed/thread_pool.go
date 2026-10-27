package main

import (
	"encoding/json"
	"net/http"
)

type threadPoolResponse struct {
	Threads []ThreadPoolEntry `json:"threads"`
}

func writeThreadPool(w http.ResponseWriter, entries []ThreadPoolEntry) {
	if entries == nil {
		entries = []ThreadPoolEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(threadPoolResponse{Threads: entries})
}
