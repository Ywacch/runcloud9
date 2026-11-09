// Package signals declares PromQL signals and renders cluster and namespace forms.
// It does not query Prometheus.
package signals

type ID string

const (
	AppRequestRate   ID = "app_request_rate"
	AppErrorRate     ID = "app_error_rate"
	AppLatencyP50    ID = "app_latency_p50"
	AppLatencyP95    ID = "app_latency_p95"
	AppLatencyP99    ID = "app_latency_p99"
	AppProcessCPU    ID = "app_process_cpu"
	AppProcessMemory ID = "app_process_memory"
)

type Shape int

const (
	PerNamespace Shape = iota
	PerApp
	PerPod
	PerBucket
)

type Missing int

const (
	MissingIsZero Missing = iota
	MissingIsUnknown
)

// Signal describes one PromQL value the metrics collector may read via the hub or direct query.
type Signal struct {
	ID      ID
	Expr    string
	Shape   Shape
	Missing Missing
	Metrics []string
}
