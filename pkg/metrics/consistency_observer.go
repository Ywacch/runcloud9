package metrics

import "github.com/ywacch/cloud9/pkg/probes"

// consistencyObserver runs background primary/replica consistency probes per namespace.
type consistencyObserver interface {
	Start(namespace string)
	Stop(namespace string)
	Latest(namespace string) (probes.ProbeResult, bool)
}

// consistencyProbeReader supplies the latest probe snapshot for invariant evaluation.
type consistencyProbeReader interface {
	Latest(namespace string) (probes.ProbeResult, bool)
}
