package phenomena

const phenomenonReplyBeforeParent = "reply_before_parent"

// ReplyBeforeParentInput is everything the entry needs. Pure data; no I/O.
type ReplyBeforeParentInput struct {
	AppReads, AppOrphanReads                int64
	ObserverWindowReads, ObserverViolations int64
	ObserverExamples                        []Example
	DelaysMs                                []int
	ReadConsistency                         string
	ReadReplicas                            int
	RunRole                                 string
	DetectionMayBeIncomplete                bool
}

// BuildReplyBeforeParent returns nil when there were no thread reads.
func BuildReplyBeforeParent(in ReplyBeforeParentInput) *Entry {
	observerRan := in.ObserverWindowReads > 0 || in.ObserverViolations > 0
	if in.AppReads <= 0 && in.ObserverWindowReads <= 0 {
		return nil
	}

	entry := &Entry{
		Phenomenon:               phenomenonReplyBeforeParent,
		RunRole:                  in.RunRole,
		Fidelity:                 "simulated",
		Cause:                    map[string]any{},
		Sources:                  sourcesFor(in, observerRan),
		DetectionMayBeIncomplete: in.DetectionMayBeIncomplete,
		Expected:                 ptrRange(expectedReplyBeforeParent(in.DelaysMs)),
	}
	if in.ReadConsistency != "" {
		entry.Cause["readConsistency"] = in.ReadConsistency
	}
	if len(in.DelaysMs) > 0 {
		entry.Cause["replicaApplyDelayMs"] = in.DelaysMs
	}
	if in.ReadReplicas > 0 {
		entry.Cause["readReplicas"] = in.ReadReplicas
	}

	affected := in.AppOrphanReads
	if observerRan && in.ObserverViolations > affected {
		affected = in.ObserverViolations
	}
	entry.Cost.ReadsAffected = affected

	if observerRan {
		entry.BaseRate = BaseRate{
			PerThousand: perThousand(in.ObserverViolations, in.ObserverWindowReads),
			Denominator: "observer thread reads in window",
			Reads:       in.ObserverWindowReads,
		}
		if in.AppReads > 0 {
			entry.Cause["allReadsPerThousand"] = perThousand(in.AppOrphanReads, in.AppReads)
		}
	} else {
		entry.BaseRate = BaseRate{
			PerThousand: perThousand(in.AppOrphanReads, in.AppReads),
			Denominator: "all thread reads",
			Reads:       in.AppReads,
		}
	}

	if len(in.ObserverExamples) > 0 {
		entry.Examples = append([]Example(nil), in.ObserverExamples...)
	}
	return entry
}

// expectedReplyBeforeParent derives the lab acceptance band from staggered replica delays
// (lab-realism-pedagogy plan §4, VS-4 observer window): with at least two standbys and
// stagger ≥500 ms, expect roughly 150–400 orphan sightings per 1,000 window reads;
// uniform delays keep the rate near zero.
func expectedReplyBeforeParent(delays []int) RateRange {
	if len(delays) >= 2 {
		min, max := delays[0], delays[0]
		for _, d := range delays[1:] {
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}
		if max-min >= 500 {
			return RateRange{MinPerThousand: 150, MaxPerThousand: 400}
		}
	}
	return RateRange{MinPerThousand: 0, MaxPerThousand: 5}
}

func sourcesFor(in ReplyBeforeParentInput, observerRan bool) []string {
	var out []string
	if in.AppReads > 0 || in.AppOrphanReads > 0 {
		out = append(out, "app")
	}
	if observerRan {
		out = append(out, "observer")
	}
	return out
}

func perThousand(affected, reads int64) float64 {
	if reads <= 0 {
		return 0
	}
	return float64(affected) / float64(reads) * 1000
}

func ptrRange(r RateRange) *RateRange {
	return &r
}
