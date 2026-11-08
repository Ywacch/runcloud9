package signals

// promRateWindow matches pkg/metrics.PromRateWindow ("10s"). Duplicated so this package
// does not import pkg/metrics.
const promRateWindow = "[10s]"

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
	}
}
