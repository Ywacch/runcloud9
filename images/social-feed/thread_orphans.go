package main

// threadHasOrphan reports whether any reply's parent id is missing from parents.
// Keep in sync with pkg/loadgen/vu/observer.go FindOrphans.
func threadHasOrphan(replies, parents []Post) bool {
	parentIDs := make(map[int]struct{}, len(parents))
	for _, p := range parents {
		parentIDs[p.ID] = struct{}{}
	}
	for _, r := range replies {
		if r.ReplyToID == nil {
			continue
		}
		if _, ok := parentIDs[*r.ReplyToID]; !ok {
			return true
		}
	}
	return false
}

func recordThreadReadMetrics(source string, replies, parents []Post) {
	socialThreadReadsTotal.WithLabelValues(source).Inc()
	if threadHasOrphan(replies, parents) {
		socialThreadOrphanReadsTotal.WithLabelValues(source).Inc()
	}
}
