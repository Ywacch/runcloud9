package phenomena

import "time"

// Entry is one phenomenon's cost and evidence for a run.
type Entry struct {
	Phenomenon               string         `json:"phenomenon"`
	RunRole                  string         `json:"runRole,omitempty"`
	Fidelity                 string         `json:"fidelity"`
	Cost                     Cost           `json:"cost"`
	BaseRate                 BaseRate       `json:"baseRate"`
	Expected                 *RateRange     `json:"expected,omitempty"`
	Examples                 []Example      `json:"examples,omitempty"`
	Cause                    map[string]any `json:"cause"`
	Sources                  []string       `json:"sources"`
	DetectionMayBeIncomplete bool           `json:"detectionMayBeIncomplete,omitempty"`
}

// Cost summarizes affected reads for one phenomenon.
type Cost struct {
	ReadsAffected   int64 `json:"readsAffected"`
	ThreadsAffected int64 `json:"threadsAffected,omitempty"`
}

// BaseRate is an observed rate with an explicit denominator label.
type BaseRate struct {
	PerThousand float64 `json:"perThousand"`
	Denominator string  `json:"denominator"`
	Reads       int64   `json:"reads"`
}

// RateRange is an expected band per thousand reads.
type RateRange struct {
	MinPerThousand float64 `json:"minPerThousand"`
	MaxPerThousand float64 `json:"maxPerThousand"`
}

// Example is one corroborating sighting.
type Example struct {
	RequestID string            `json:"requestId"`
	At        time.Time         `json:"at"`
	Detail    map[string]string `json:"detail"`
}
