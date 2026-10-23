package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	vegeta "github.com/tsenart/vegeta/v12/lib"
	"github.com/ywacch/cloud9/pkg/loadgen"
)

func TestHandleMetrics_identityFields(t *testing.T) {
	srv := &server{}
	srv.stats = loadgen.LoadStats{
		Active:        true,
		IdentityCount: 60,
		StatusCodes:   map[string]int{},
		Distribution:  map[string]int{},
	}
	srv.idCounter = loadgen.NewClientIDCounter(10000)
	for _, id := range []string{"1", "2", "1"} {
		wrapped := loadgen.CountClientIDs(func(tgt *vegeta.Target) error {
			tgt.Header = make(map[string][]string)
			tgt.Header.Set("X-C9-Client-ID", id)
			return nil
		}, srv.idCounter)
		var tgt vegeta.Target
		if err := wrapped(&tgt); err != nil {
			t.Fatal(err)
		}
	}

	rec := httptest.NewRecorder()
	srv.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	var out loadgen.LoadStats
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.IdentityCount != 60 {
		t.Fatalf("identityCount: %d", out.IdentityCount)
	}
	if out.IdentitiesSeen != 2 {
		t.Fatalf("identitiesSeen: %d", out.IdentitiesSeen)
	}
}
