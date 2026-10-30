package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/ywacch/cloud9/pkg/engines"
)

var (
	jobsCatalogBodyOnce sync.Once
	jobsCatalogBody     []byte
	jobsCatalogETag     string
)

func jobsCatalogPayload() ([]byte, string) {
	jobsCatalogBodyOnce.Do(func() {
		body, err := json.Marshal(engines.Default())
		if err != nil {
			panic("marshal jobs catalog: " + err.Error())
		}
		sum := sha256.Sum256(body)
		jobsCatalogBody = body
		jobsCatalogETag = "\"" + hex.EncodeToString(sum[:]) + "\""
	})
	return jobsCatalogBody, jobsCatalogETag
}

func (h *Handler) handleGetMetaJobs(w http.ResponseWriter, r *http.Request) {
	body, etag := jobsCatalogPayload()
	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
