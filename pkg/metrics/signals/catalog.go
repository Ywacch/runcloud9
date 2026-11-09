package signals

// promRateWindow matches pkg/metrics.PromRateWindow ("10s"). Duplicated so this package
// does not import pkg/metrics.
const promRateWindow = "[10s]"

// Lookup returns one catalog entry by ID.
func Lookup(id ID) (Signal, bool) {
	for _, s := range Catalog() {
		if s.ID == id {
			return s, true
		}
	}
	return Signal{}, false
}

// Catalog returns every declared signal. The slice order is stable for tests.
func Catalog() []Signal {
	return []Signal{
		{
			ID:      AppRequestRate,
			Shape:   PerApp,
			Missing: MissingIsZero,
			Metrics: []string{"http_requests_total"},
			Expr:    `sum by (__BY__) (rate(http_requests_total{__SEL__}` + promRateWindow + `))`,
		},
		{
			ID:      AppErrorRate,
			Shape:   PerApp,
			Missing: MissingIsZero,
			Metrics: []string{"http_requests_total"},
			Expr:    `sum by (__BY__) (rate(http_requests_total{__SEL__,status=~"5.."}` + promRateWindow + `))`,
		},
		{
			ID:      AppLatencyP50,
			Shape:   PerApp,
			Missing: MissingIsUnknown,
			Metrics: []string{"http_request_duration_seconds_bucket"},
			Expr:    `histogram_quantile(0.50, sum by (__BY__, le) (rate(http_request_duration_seconds_bucket{__SEL__}` + promRateWindow + `)))`,
		},
		{
			ID:      AppLatencyP95,
			Shape:   PerApp,
			Missing: MissingIsUnknown,
			Metrics: []string{"http_request_duration_seconds_bucket"},
			Expr:    `histogram_quantile(0.95, sum by (__BY__, le) (rate(http_request_duration_seconds_bucket{__SEL__}` + promRateWindow + `)))`,
		},
		{
			ID:      AppLatencyP99,
			Shape:   PerApp,
			Missing: MissingIsUnknown,
			Metrics: []string{"http_request_duration_seconds_bucket"},
			Expr:    `histogram_quantile(0.99, sum by (__BY__, le) (rate(http_request_duration_seconds_bucket{__SEL__}` + promRateWindow + `)))`,
		},
		{
			ID:      AppProcessCPU,
			Shape:   PerApp,
			Missing: MissingIsUnknown,
			Metrics: []string{"process_cpu_seconds_total"},
			Expr:    `sum by (__BY__) (rate(process_cpu_seconds_total{__SEL__}` + promRateWindow + `))`,
		},
		{
			ID:      AppProcessMemory,
			Shape:   PerApp,
			Missing: MissingIsUnknown,
			Metrics: []string{"process_resident_memory_bytes"},
			Expr:    `sum by (__BY__) (process_resident_memory_bytes{__SEL__})`,
		},
		{
			ID:      AppCacheHitsRate,
			Shape:   PerApp,
			Missing: MissingIsZero,
			Metrics: []string{"cache_hits_total"},
			Expr:    `sum by (__BY__) (rate(cache_hits_total{__SEL__}` + promRateWindow + `))`,
		},
		{
			ID:      AppCacheMissesRate,
			Shape:   PerApp,
			Missing: MissingIsZero,
			Metrics: []string{"cache_misses_total"},
			Expr:    `sum by (__BY__) (rate(cache_misses_total{__SEL__}` + promRateWindow + `))`,
		},
		{
			ID:      AppCacheErrorsRate,
			Shape:   PerApp,
			Missing: MissingIsZero,
			Metrics: []string{"cache_errors_total"},
			Expr:    `sum by (__BY__) (rate(cache_errors_total{__SEL__}` + promRateWindow + `))`,
		},
		{
			ID:      PgDatabaseXactCommitRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_commit"},
			Expr:    `sum by (__BY__) (rate(pg_stat_database_xact_commit{__SEL__` + pgDatabaseDatnameSel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgDatabaseXactRollbackRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_rollback"},
			Expr:    `sum by (__BY__) (rate(pg_stat_database_xact_rollback{__SEL__` + pgDatabaseDatnameSel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgDatabaseXactCommitRatePod,
			Shape:   PerPod,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_commit"},
			Expr:    `sum by (__BY__) (rate(pg_stat_database_xact_commit{__SEL__` + pgDatabaseDatnameSel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgDatabaseXactRollbackRatePod,
			Shape:   PerPod,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_rollback"},
			Expr:    `sum by (__BY__) (rate(pg_stat_database_xact_rollback{__SEL__` + pgDatabaseDatnameSel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgDatabaseXactCommitTotal,
			Shape:   PerPod,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_commit"},
			Expr:    `sum by (__BY__) (pg_stat_database_xact_commit{__SEL__` + pgDatabaseDatnameSel + `})`,
		},
		{
			ID:      PgDatabaseXactRollbackTotal,
			Shape:   PerPod,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_database_xact_rollback"},
			Expr:    `sum by (__BY__) (pg_stat_database_xact_rollback{__SEL__` + pgDatabaseDatnameSel + `})`,
		},
		{
			ID:      PgUserTableSeqTupReadRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_seq_tup_read"},
			Expr:    `sum by (__BY__) (rate(pg_stat_user_tables_seq_tup_read{__SEL__` + pgUserTablePrimarySel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgUserTableIdxTupFetchRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_idx_tup_fetch"},
			Expr:    `sum by (__BY__) (rate(pg_stat_user_tables_idx_tup_fetch{__SEL__` + pgUserTablePrimarySel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgUserTableNTupInsRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_ins"},
			Expr:    `sum by (__BY__) (rate(pg_stat_user_tables_n_tup_ins{__SEL__` + pgUserTablePrimarySel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgUserTableNTupUpdRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_upd"},
			Expr:    `sum by (__BY__) (rate(pg_stat_user_tables_n_tup_upd{__SEL__` + pgUserTablePrimarySel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgUserTableNTupDelRate,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_del"},
			Expr:    `sum by (__BY__) (rate(pg_stat_user_tables_n_tup_del{__SEL__` + pgUserTablePrimarySel + `}` + promRateWindow + `))`,
		},
		{
			ID:      PgUserTableSeqTupReadTotal,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_seq_tup_read"},
			Expr:    `sum by (__BY__) (pg_stat_user_tables_seq_tup_read{__SEL__` + pgUserTablePrimarySel + `})`,
		},
		{
			ID:      PgUserTableIdxTupFetchTotal,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_idx_tup_fetch"},
			Expr:    `sum by (__BY__) (pg_stat_user_tables_idx_tup_fetch{__SEL__` + pgUserTablePrimarySel + `})`,
		},
		{
			ID:      PgUserTableNTupInsTotal,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_ins"},
			Expr:    `sum by (__BY__) (pg_stat_user_tables_n_tup_ins{__SEL__` + pgUserTablePrimarySel + `})`,
		},
		{
			ID:      PgUserTableNTupUpdTotal,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_upd"},
			Expr:    `sum by (__BY__) (pg_stat_user_tables_n_tup_upd{__SEL__` + pgUserTablePrimarySel + `})`,
		},
		{
			ID:      PgUserTableNTupDelTotal,
			Shape:   PerNamespace,
			Missing: MissingIsZero,
			Metrics: []string{"pg_stat_user_tables_n_tup_del"},
			Expr:    `sum by (__BY__) (pg_stat_user_tables_n_tup_del{__SEL__` + pgUserTablePrimarySel + `})`,
		},
	}
}
