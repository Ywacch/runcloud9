package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/ywacch/cloud9/pkg/rules"
)

type rulesCatalogResponse struct {
	CatalogVersion string     `json:"catalogVersion"`
	Rules          []ruleView `json:"rules"`
}

type ruleView struct {
	ID             string   `json:"id"`
	Family         string   `json:"family"`
	Kind           string   `json:"kind,omitempty"`
	Component      string   `json:"component,omitempty"`
	Visibility     string   `json:"visibility"`
	Summary        string   `json:"summary"`
	CausedByConfig []string `json:"causedByConfig,omitempty"`
}

var (
	rulesCatalogBodyOnce sync.Once
	rulesCatalogBody     []byte
	rulesCatalogETag     string
)

func rulesCatalogPayload() ([]byte, string) {
	rulesCatalogBodyOnce.Do(func() {
		static := rules.All()
		views := make([]ruleView, len(static))
		for i, r := range static {
			views[i] = ruleView{
				ID:             r.ID,
				Family:         string(r.Family),
				Kind:           r.Kind,
				Component:      r.Component,
				Visibility:     r.Visibility,
				Summary:        r.Summary,
				CausedByConfig: r.CausedByConfig,
			}
		}
		resp := rulesCatalogResponse{
			CatalogVersion: rules.CatalogVersion,
			Rules:          views,
		}
		body, err := json.Marshal(resp)
		if err != nil {
			panic("marshal rules catalog: " + err.Error())
		}
		sum := sha256.Sum256(body)
		rulesCatalogBody = body
		rulesCatalogETag = "\"" + hex.EncodeToString(sum[:]) + "\""
	})
	return rulesCatalogBody, rulesCatalogETag
}

func etagMatches(ifNoneMatch, etag string) bool {
	if ifNoneMatch == "" || etag == "" {
		return false
	}
	for _, part := range strings.Split(ifNoneMatch, ",") {
		if strings.TrimSpace(part) == etag {
			return true
		}
	}
	return false
}

func (h *Handler) handleGetRules(w http.ResponseWriter, r *http.Request) {
	body, etag := rulesCatalogPayload()
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
