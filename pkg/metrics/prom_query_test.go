package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type promQueryRecord struct {
	path, caller, template string
}

func TestExecutePromInstant_CountsDirectQuery(t *testing.T) {
	t.Parallel()

	var calls []promQueryRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     []any{},
			},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetPromQueryObserver(func(path, caller, template string) {
		calls = append(calls, promQueryRecord{path, caller, template})
	})

	ctx := WithPromMeta(context.Background(), "url-shortener-small", "storage", "direct")
	_, err := c.executePromInstant(ctx, `up`)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls: want 1, got %d", len(calls))
	}
	if calls[0].path != "direct" || calls[0].caller != "storage" || calls[0].template != "url-shortener-small" {
		t.Fatalf("got %+v", calls[0])
	}
}

func TestExecutePromInstant_SamplePath(t *testing.T) {
	t.Parallel()

	var calls []promQueryRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     []any{},
			},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetPromQueryObserver(func(path, caller, template string) {
		calls = append(calls, promQueryRecord{path, caller, template})
	})

	ctx := WithPromMeta(context.Background(), "", "collect", "cluster_sample")
	_, err := c.executePromInstant(ctx, `up`)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].path != "cluster_sample" {
		t.Fatalf("calls: want cluster_sample once, got %+v", calls)
	}
	if calls[0].template != "none" || calls[0].caller != "collect" {
		t.Fatalf("defaults: got %+v", calls[0])
	}
}

func TestExecutePromInstant_EmptyURLDoesNotCount(t *testing.T) {
	t.Parallel()

	called := false
	c := NewMetricsCollector("", nil, nil, nil, nil)
	c.SetPromQueryObserver(func(path, caller, template string) {
		called = true
	})

	ctx := WithPromMeta(context.Background(), "url-shortener-small", "collect", "direct")
	_, err := c.executePromInstant(ctx, `up`)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("observer called with empty prometheus URL")
	}
}

func TestExecutePromInstant_DirectAndSamplePathsDiffer(t *testing.T) {
	t.Parallel()

	var calls []promQueryRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     []any{},
			},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetPromQueryObserver(func(path, caller, template string) {
		calls = append(calls, promQueryRecord{path, caller, template})
	})

	_, _ = c.executePromInstant(WithPromMeta(context.Background(), "t", "collect", "direct"), `up`)
	_, _ = c.executePromInstant(WithPromMeta(context.Background(), "", "collect", "cluster_sample"), `up`)
	if len(calls) != 2 {
		t.Fatalf("calls: want 2, got %d", len(calls))
	}
	if calls[0].path == calls[1].path {
		t.Fatalf("paths should differ: %+v", calls)
	}
}
