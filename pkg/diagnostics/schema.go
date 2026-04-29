package diagnostics

// FailureSignature is a normalized diagnostic finding extracted from events/logs/errors.
type FailureSignature struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Source   string `json:"source"`
	Severity string `json:"severity"`
}

// DiagnosticArtifact is the persisted evidence bundle for a deployment namespace.
type DiagnosticArtifact struct {
	SchemaVersion         int                  `json:"schema_version"`
	CollectedAt           string               `json:"collected_at"`
	Namespace             string               `json:"namespace"`
	Template              string               `json:"template"`
	Outcome               string               `json:"outcome"`
	OrchestrationError    string               `json:"orchestration_error,omitempty"`
	Signatures            []FailureSignature   `json:"failure_signatures"`
	Pods                  []PodSnapshot        `json:"pods"`
	Deployments           []DeploymentSnapshot `json:"deployments"`
	Services              []ServiceSnapshot    `json:"services"`
	Events                []EventSnapshot      `json:"events"`
	Logs                  map[string][]string  `json:"logs"`
	APILogs               []string             `json:"api_logs"`
	CollectionDuration    string               `json:"collection_duration"`
	Partial               bool                 `json:"partial"`
	CollectionWarnings    []string             `json:"collection_warnings,omitempty"`
	PodsTruncated         bool                 `json:"pods_truncated,omitempty"`
	DeploymentsTruncated  bool                 `json:"deployments_truncated,omitempty"`
	ServicesTruncated     bool                 `json:"services_truncated,omitempty"`
	LogsDropped           bool                 `json:"logs_dropped,omitempty"`
	EventsCollectionError string               `json:"events_collection_error,omitempty"`
}

// PodSnapshot contains a scrubbed pod status summary (no env vars or secrets).
type PodSnapshot struct {
	Name       string            `json:"name"`
	Phase      string            `json:"phase"`
	Ready      bool              `json:"ready"`
	Conditions []string          `json:"conditions"`
	Node       string            `json:"node"`
	Containers []ContainerStatus `json:"containers"`
	Age        string            `json:"age"`
}

// ContainerStatus is the safe per-container status included in diagnostics.
type ContainerStatus struct {
	Name         string `json:"name"`
	Image        string `json:"image"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	State        string `json:"state"`
}

// DeploymentSnapshot summarizes deployment readiness state.
type DeploymentSnapshot struct {
	Name              string `json:"name"`
	Replicas          int32  `json:"replicas"`
	AvailableReplicas int32  `json:"available_replicas"`
	Ready             bool   `json:"ready"`
}

// ServiceSnapshot summarizes service exposure.
type ServiceSnapshot struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Ports []int32 `json:"ports"`
}

// EventSnapshot is a scrubbed namespace event row.
type EventSnapshot struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Type      string `json:"type"`
	Object    string `json:"object"`
	Count     int32  `json:"count"`
	Timestamp string `json:"timestamp"`
}

// CollectParams drives one diagnostics collection run.
type CollectParams struct {
	Namespace string
	Template  string
	Outcome   string
	OrcError  string
}
