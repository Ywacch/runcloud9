package metrics

import "fmt"

// clusterPromScalarQueries lists PromQL executed once per refresh (sum by (namespace)).
func clusterPromScalarQueries(rateRange string) []string {
	rr := rateRange
	return []string{
		fmt.Sprintf(`sum by (namespace) (sum(rate(http_requests_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(cache_hits_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(cache_misses_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(cache_errors_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(db_query_duration_seconds_count{pod=~"app-server.*"}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(db_query_duration_seconds_sum{pod=~"app-server.*"}%s)))`, rr),
		`sum by (namespace) (max(pg_settings{name="max_connections"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(pg_stat_database_xact_commit{datname!=""}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(pg_stat_database_xact_rollback{datname!=""}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(process_cpu_seconds_total{pod=~"app-server.*"}%s)))`, rr),
		`sum by (namespace) (sum(process_resident_memory_bytes{pod=~"app-server.*"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(redis_cpu_user_seconds_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(redis_cpu_sys_seconds_total%s)))`, rr),
		`sum by (namespace) (sum(process_resident_memory_bytes{pod=~"cache-proxy.*"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(process_cpu_seconds_total{pod=~"cache-proxy.*"}%s)))`, rr),
		`sum by (namespace) (sum(redis_connected_clients))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(redis_keyspace_hits_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(redis_keyspace_misses_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(redis_rejected_connections_total%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate({__name__=~".*_mq_publish_total"}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate({__name__=~".*_worker_processed_total"}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate({__name__=~".*_worker_errors_total"}%s)))`, rr),
		fmt.Sprintf(`sum by (namespace) (sum(rate(container_cpu_usage_seconds_total{container="postgres"}%s)))`, rr),
		`sum by (namespace) (sum(container_memory_working_set_bytes{container="postgres"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(container_cpu_usage_seconds_total{container="rabbitmq"}%s)))`, rr),
		`sum by (namespace) (sum(container_memory_working_set_bytes{container="rabbitmq"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(container_cpu_usage_seconds_total{container="nginx"}%s)))`, rr),
		`sum by (namespace) (sum(container_memory_working_set_bytes{container="nginx"}))`,
		fmt.Sprintf(`sum by (namespace) (sum(rate(process_cpu_seconds_total{pod=~"worker.*"}%s)))`, rr),
		`sum by (namespace) (sum(process_resident_memory_bytes{pod=~"worker.*"}))`,
		`sum by (namespace) (sum(consistency_read_stale_total))`,
	}
}

func clusterPromVectorQueries(rateRange string) []string {
	rr := rateRange
	return []string{
		fmt.Sprintf(`sum by (namespace, pod) (sum(rate(http_requests_total{pod=~"app-server.*"}%s)))`, rr),
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
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (sum(rate(http_request_duration_seconds_bucket%s)))`, rr),
		},
		{
			key:            "worker_processing_duration_seconds_bucket",
			metricFragment: "_worker_processing_duration_seconds_bucket",
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (sum(rate({__name__=~".*_worker_processing_duration_seconds_bucket"}%s)))`, rr),
		},
		{
			key:            "redis_commands_latencies_usec_bucket",
			metricFragment: "redis_commands_latencies_usec_bucket",
			clusterQL:      fmt.Sprintf(`sum by (namespace, le) (sum(rate(redis_commands_latencies_usec_bucket%s)))`, rr),
		},
	}
}
