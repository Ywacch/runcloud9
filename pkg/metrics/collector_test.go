package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCollect_CacheIncreaseQueries(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		var val string
		switch {
		case strings.Contains(q, "increase(cache_hits_total"):
			val = "150"
		case strings.Contains(q, "increase(cache_misses_total"):
			val = "30"
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": "success",
				"data": map[string]any{
					"resultType": "vector",
					"result":     []any{},
				},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []map[string]any{
					{
						"metric": map[string]string{},
						"value":  []any{float64(1), val},
					},
				},
			},
		})
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL)
	dm, err := c.Collect(context.Background(), "ns-test")
	if err != nil {
		t.Fatal(err)
	}
	app := dm.Components["app-server"]
	if app.CacheHitsLast9m != 150 {
		t.Fatalf("CacheHitsLast9m: want 150, got %v", app.CacheHitsLast9m)
	}
	if app.CacheMissesLast9m != 30 {
		t.Fatalf("CacheMissesLast9m: want 30, got %v", app.CacheMissesLast9m)
	}
}
