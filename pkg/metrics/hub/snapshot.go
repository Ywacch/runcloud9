package hub

import (
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

// Snapshot is immutable after publish. Readers keep the pointer only.
type Snapshot struct {
	At time.Time

	perNS    map[signals.ID]map[string]float64            // signal -> namespace -> value
	perApp   map[signals.ID]map[string]map[string]float64 // signal -> namespace -> app -> value
	perPod   map[signals.ID]map[string]map[string]float64
	perLe    map[signals.ID]map[string]map[string]map[string]float64 // signal -> ns -> app -> le -> value
	meta     map[signals.ID]signalMeta
	podUsage metricsAPIState
}

type signalMeta struct {
	FetchedAt time.Time
	Err       bool
	Stale     bool
	Absent    bool
}

type metricsAPIState struct {
	At    time.Time
	ByPod map[string]map[string]map[string]containerUsage
}

type containerUsage struct {
	CPUMilli int64
	MemBytes int64
}

func newSnapshot(at time.Time) *Snapshot {
	return &Snapshot{
		At:     at,
		perNS:  make(map[signals.ID]map[string]float64),
		perApp: make(map[signals.ID]map[string]map[string]float64),
		perPod: make(map[signals.ID]map[string]map[string]float64),
		perLe:  make(map[signals.ID]map[string]map[string]map[string]float64),
		meta:   make(map[signals.ID]signalMeta),
		podUsage: metricsAPIState{
			ByPod: make(map[string]map[string]map[string]containerUsage),
		},
	}
}

// ByApp returns app-label values for one namespace and signal. absent is false when nothing was present.
// stale is true when the value came from a previous refresh after a failed fetch.
func (s *Snapshot) ByApp(namespace string, id signals.ID) (values map[string]float64, present bool, stale bool) {
	if s == nil || namespace == "" {
		return nil, false, false
	}
	m, ok := s.perApp[id]
	if !ok {
		p, st := metaStaleAbsent(s, id)
		return nil, p, st
	}
	inner, ok := m[namespace]
	if !ok || len(inner) == 0 {
		p, st := metaStaleAbsent(s, id)
		return nil, p, st
	}
	out := make(map[string]float64, len(inner))
	for k, v := range inner {
		out[k] = v
	}
	meta := s.meta[id]
	return out, !meta.Absent, meta.Stale
}

func metaStaleAbsent(s *Snapshot, id signals.ID) (present bool, stale bool) {
	if s == nil {
		return false, false
	}
	meta := s.meta[id]
	return !meta.Absent, meta.Stale
}
