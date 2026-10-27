package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteThreadPool_NilIsEmptyArray(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	writeThreadPool(rec, nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: got %q", ct)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	raw := body["threads"]
	if raw == nil {
		t.Fatal("missing threads key")
	}
	if string(raw) != "[]" {
		t.Fatalf("threads: got %s want []", raw)
	}
}

func TestWriteThreadPool_RowShape(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	writeThreadPool(rec, []ThreadPoolEntry{{RootID: 1234, ReplyCount: 6}})

	var parsed struct {
		Threads []ThreadPoolEntry `json:"threads"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(parsed.Threads) != 1 {
		t.Fatalf("threads len: got %d want 1", len(parsed.Threads))
	}
	if parsed.Threads[0].RootID != 1234 || parsed.Threads[0].ReplyCount != 6 {
		t.Fatalf("row: got %+v", parsed.Threads[0])
	}
}
