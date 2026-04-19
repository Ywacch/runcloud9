package metrics

// RingBuffer stores the last N per-tick DeploymentMetrics snapshots for a namespace.
//
// Memory: a DeploymentMetrics value with components, template metrics, and events is on the
// order of a few KB when serialized; 300 entries ≈ ~0.6–1.5MB per deployment. At tens of
// concurrent deployments this stays within a single API process; monitor if session counts grow.
type RingBuffer struct {
	buf  [300]DeploymentMetrics // 300 × ~2s ≈ 10 minutes at the default scrape cadence
	head int
	size int
}

// Add inserts a snapshot; when full, the oldest entry is dropped.
func (rb *RingBuffer) Add(dm DeploymentMetrics) {
	if rb == nil {
		return
	}
	rb.buf[rb.head] = dm
	rb.head = (rb.head + 1) % len(rb.buf)
	if rb.size < len(rb.buf) {
		rb.size++
	}
}

// Len returns how many snapshots are stored (≤ 300).
func (rb *RingBuffer) Len() int {
	if rb == nil {
		return 0
	}
	return rb.size
}

// All returns snapshots from oldest to newest.
func (rb *RingBuffer) All() []DeploymentMetrics {
	if rb == nil || rb.size == 0 {
		return nil
	}
	out := make([]DeploymentMetrics, 0, rb.size)
	if rb.size < len(rb.buf) {
		for i := 0; i < rb.size; i++ {
			out = append(out, rb.buf[i])
		}
		return out
	}
	for i := 0; i < rb.size; i++ {
		out = append(out, rb.buf[(rb.head+i)%len(rb.buf)])
	}
	return out
}

// DownsampledHistory returns a smaller slice for JSON persistence: every 4th snapshot plus
// any snapshot that carries events (threshold, cascade, actions) so annotated moments are kept.
func (rb *RingBuffer) DownsampledHistory() []DeploymentMetrics {
	if rb == nil {
		return nil
	}
	all := rb.All()
	var result []DeploymentMetrics
	for i, snap := range all {
		if i%4 == 0 || len(snap.Events) > 0 {
			result = append(result, snap)
		}
	}
	return result
}
