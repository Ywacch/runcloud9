package signals

// Selectors for replica-table PromQL (bare namespace or app/worker pod regex).
const (
	appServerPodSel    = `,pod=~"app-server.*"`
	workerPodSel       = `,pod=~"worker.*"`
	workerProcessedSel = `{__name__=~".*_worker_processed_total",`
	workerErrorsSel    = `{__name__=~".*_worker_errors_total",`
	workerDurationSel  = `{__name__=~".*_worker_processing_duration_seconds_bucket",`
)
