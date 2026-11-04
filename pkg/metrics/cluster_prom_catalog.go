package metrics

import "fmt"

// clusterPromScalarQueries lists PromQL executed once per refresh.
// The grouping clause must sit on the aggregation that still has the namespace label.
// An inner sum() or max() with no by clause drops that label, and the sample stores nothing.
func clusterPromScalarQueries(rateRange string) []string {
	rr := rateRange
	return []string{
		fmt.Sprintf(`sum by (namespace) (rate(http_requests_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(http_requests_total{status=~"5.."}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(cache_hits_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(cache_misses_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(cache_errors_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(db_query_duration_seconds_count{pod=~"app-server.*"}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(db_query_duration_seconds_sum{pod=~"app-server.*"}%s))`, rr),
		`max by (namespace) (pg_settings{name="max_connections"})`,
		fmt.Sprintf(`sum by (namespace) (rate(pg_stat_database_xact_commit{datname!=""}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(pg_stat_database_xact_rollback{datname!=""}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(process_cpu_seconds_total{pod=~"app-server.*"}%s))`, rr),
		`sum by (namespace) (process_resident_memory_bytes{pod=~"app-server.*"})`,
		fmt.Sprintf(`sum by (namespace) (rate(redis_cpu_user_seconds_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(redis_cpu_sys_seconds_total%s))`, rr),
		`sum by (namespace) (process_resident_memory_bytes{pod=~"cache-proxy.*"})`,
		fmt.Sprintf(`sum by (namespace) (rate(process_cpu_seconds_total{pod=~"cache-proxy.*"}%s))`, rr),
		`sum by (namespace) (redis_connected_clients)`,
		fmt.Sprintf(`sum by (namespace) (rate(redis_keyspace_hits_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(redis_keyspace_misses_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(redis_rejected_connections_total%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate({__name__=~".*_mq_publish_total"}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate({__name__=~".*_worker_processed_total"}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate({__name__=~".*_worker_errors_total"}%s))`, rr),
		fmt.Sprintf(`sum by (namespace) (rate(container_cpu_usage_seconds_total{container="postgres"}%s))`, rr),
		`sum by (namespace) (container_memory_working_set_bytes{container="postgres"})`,
		fmt.Sprintf(`sum by (namespace) (rate(container_cpu_usage_seconds_total{container="rabbitmq"}%s))`, rr),
		`sum by (namespace) (container_memory_working_set_bytes{container="rabbitmq"})`,
		fmt.Sprintf(`sum by (namespace) (rate(container_cpu_usage_seconds_total{container="nginx"}%s))`, rr),
		`sum by (namespace) (container_memory_working_set_bytes{container="nginx"})`,
		fmt.Sprintf(`sum by (namespace) (rate(process_cpu_seconds_total{pod=~"worker.*"}%s))`, rr),
		`sum by (namespace) (process_resident_memory_bytes{pod=~"worker.*"})`,
		`sum by (namespace) (consistency_read_stale_total)`,
	}
}

func clusterPromVectorQueries(rateRange string) []string {
	rr := rateRange
	return []string{
		fmt.Sprintf(`sum by (namespace, pod) (rate(http_requests_total{pod=~"app-server.*"}%s))`, rr),
	}
}

type bucketCatalogEntry struct {
	key            string
	metricFragment string
	clusterQL      string
}

func clusterPromBucketQueries(rateRange string) []bucketCatalogEntry {
	rr := rateRange
	return []bucketCatalogEntry{
		{
			key:            "http_request_duration_seconds_bucket",
			metricFragment: "http_request_duration_seconds_bucket",
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (rate(http_request_duration_seconds_bucket%s))`, rr),
		},
		{
			key:            "worker_processing_duration_seconds_bucket",
			metricFragment: "_worker_processing_duration_seconds_bucket",
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (rate({__name__=~".*_worker_processing_duration_seconds_bucket"}%s))`, rr),
		},
		{
			key:            "redis_commands_latencies_usec_bucket",
			metricFragment: "redis_commands_latencies_usec_bucket",
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (rate(redis_commands_latencies_usec_bucket%s))`, rr),
		},
	}
}
