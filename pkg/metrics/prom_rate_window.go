package metrics

// PromRateWindow is the PromQL range vector for live rate() and histogram bucket rates.
// Requires Prometheus scrapeInterval <= 5s (see k8s/helm/prometheus-values.yaml).
const PromRateWindow = "10s"

// promRateRange returns the bracketed range vector, e.g. "[10s]".
func promRateRange() string {
	return "[" + PromRateWindow + "]"
}
