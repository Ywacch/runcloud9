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

// ByPod returns per-pod values for one namespace and signal.
func (s *Snapshot) ByPod(namespace string, id signals.ID) (values map[string]float64, present bool, stale bool) {
	if s == nil || namespace == "" {
		return nil, false, false
	}
	m, ok := s.perPod[id]
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

// Scalar returns one float for a namespace from snapshot data (sums PerApp and PerPod maps).
func (s *Snapshot) Scalar(namespace string, id signals.ID, shape signals.Shape) (float64, bool, bool) {
	if s == nil || namespace == "" {
		return 0, false, false
	}
	switch shape {
	case signals.PerNamespace:
		m, ok := s.perNS[id]
		if !ok {
			p, st := metaStaleAbsent(s, id)
			return 0, p, st
		}
		v, ok := m[namespace]
		if !ok {
			p, st := metaStaleAbsent(s, id)
			return 0, p, st
		}
		meta := s.meta[id]
		return v, true, meta.Stale
	case signals.PerApp:
		apps, present, stale := s.ByApp(namespace, id)
		if !present || len(apps) == 0 {
			return 0, false, stale
		}
		var sum float64
		for _, v := range apps {
			sum += v
		}
		return sum, true, stale
	case signals.PerPod:
		pods, present, stale := s.ByPod(namespace, id)
		if !present || len(pods) == 0 {
			return 0, false, stale
		}
		var sum float64
		for _, v := range pods {
			sum += v
		}
		return sum, true, stale
	default:
		return 0, false, false
	}
}

// ContainerUsage sums metrics.k8s.io CPU millicores and memory bytes for one container
// in a namespace. include, when non-nil, keeps pods it accepts. exclude, when non-nil, drops them.
// sampled is true when this snapshot holds a cluster metrics API fetch.
// found is true when at least one container matched.
func (s *Snapshot) ContainerUsage(namespace, container string, include, exclude func(string) bool) (cpuMilli, memBytes int64, found, sampled bool) {
	if s == nil || s.podUsage.At.IsZero() {
		return 0, 0, false, false
	}
	if namespace == "" || container == "" {
		return 0, 0, false, true
	}
	for name, conts := range s.podUsage.ByPod[namespace] {
		if include != nil && !include(name) {
			continue
		}
		if exclude != nil && exclude(name) {
			continue
		}
		u, ok := conts[container]
		if !ok {
			continue
		}
		found = true
		cpuMilli += u.CPUMilli
		memBytes += u.MemBytes
	}
	return cpuMilli, memBytes, found, true
}
