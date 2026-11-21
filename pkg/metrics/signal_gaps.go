package metrics

// SignalGap records a component metric field that is absent by design or because
// the exporter did not return a series (E-23c). Missing is never zero.
type SignalGap struct {
	Field        string `json:"field"`
	Availability string `json:"availability"`
	Reason       string `json:"reason,omitempty"`
	Concept      string `json:"concept,omitempty"`
}

func mergeSignalGaps(dst map[string][]SignalGap, component string, gaps []SignalGap) {
	if len(gaps) == 0 {
		return
	}
	if dst == nil {
		return
	}
	dst[component] = append(dst[component], gaps...)
}

func gapNotBuilt(field, series string) SignalGap {
	reason := "redis_exporter did not return " + series
	if series == "" {
		reason = "metric series not available"
	}
	return SignalGap{
		Field:        field,
		Availability: "not_built",
		Reason:       reason,
	}
}

func gapFromPair(field, availability, reason, concept string) SignalGap {
	return SignalGap{
		Field:        field,
		Availability: availability,
		Reason:       reason,
		Concept:      concept,
	}
}
