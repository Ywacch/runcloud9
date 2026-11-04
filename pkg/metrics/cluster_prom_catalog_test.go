package metrics

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// innerAggWithoutBy matches PromQL where an outer "by (namespace…)" wraps an inner
// sum/max/min/avg that has no grouping clause. That inner step drops the namespace label.
var innerAggWithoutBy = regexp.MustCompile(`by \([^)]*namespace[^)]*\) \((sum|max|min|avg)\(`)

func TestClusterPromCatalog_NoInnerAggWithoutBy(t *testing.T) {
	t.Parallel()
	rr := promRateRange()
	var queries []string
	queries = append(queries, clusterPromScalarQueries(rr)...)
	queries = append(queries, clusterPromVectorQueries(rr)...)
	for _, spec := range clusterPromBucketQueries(rr) {
		queries = append(queries, spec.clusterQL)
	}
	for _, q := range queries {
		if innerAggWithoutBy.MatchString(q) {
			t.Fatalf("catalog query drops namespace label: %q", q)
		}
	}
}

func TestClusterPromCatalog_EveryQueryGroupsByNamespace(t *testing.T) {
	t.Parallel()
	rr := promRateRange()
	var queries []string
	queries = append(queries, clusterPromScalarQueries(rr)...)
	queries = append(queries, clusterPromVectorQueries(rr)...)
	for _, spec := range clusterPromBucketQueries(rr) {
		queries = append(queries, spec.clusterQL)
	}
	for _, q := range queries {
		if !strings.Contains(q, "by (namespace") {
			t.Fatalf("catalog query must group by namespace: %q", q)
		}
	}
}

func TestClusterPromCatalog_CollectScalarKeysInCatalog(t *testing.T) {
	t.Parallel()
	rr := promRateRange()
	catalog := make(map[string]struct{})
	for _, q := range clusterPromScalarQueries(rr) {
		catalog[q] = struct{}{}
	}
	ns := "ns-catalog-key"
	tests := []struct {
		name string
		ql   string
	}{
		{
			name: "http rps",
			ql:   fmt.Sprintf(`sum(rate(http_requests_total{namespace=%q}%s))`, ns, rr),
		},
		{
			name: "http 5xx",
			ql:   fmt.Sprintf(`sum(rate(http_requests_total{namespace=%q,status=~"5.."}%s))`, ns, rr),
		},
		{
			name: "cache hits",
			ql:   fmt.Sprintf(`sum(rate(cache_hits_total{namespace=%q}%s))`, ns, rr),
		},
		{
			name: "db qps",
			ql: fmt.Sprintf(
				`sum(rate(db_query_duration_seconds_count{%s}%s))`,
				appServerPromSelector(ns), rr,
			),
		},
		{
			name: "pg max conn",
			ql:   fmt.Sprintf(`max(pg_settings{namespace=%q,name="max_connections"})`, ns),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := promQLToClusterScalarKey(tc.ql)
			if !ok {
				t.Fatalf("promQLToClusterScalarKey failed for %q", tc.ql)
			}
			if _, inCat := catalog[key]; !inCat {
				t.Fatalf("lookup key %q not in clusterPromScalarQueries", key)
			}
		})
	}
}
