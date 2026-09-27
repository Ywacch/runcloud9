package metrics

import (
	"sort"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func appendTraceFacts(l *Ledger, samples []trace.JourneySample) {
	if l == nil || len(samples) == 0 {
		return
	}
	sorted := append([]trace.JourneySample(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].RequestID < sorted[j].RequestID
	})
	for _, s := range sorted {
		if s.RequestID == "" {
			continue
		}
		l.addFact(Fact{
			Ref:   FactRef("fact:trace:" + s.RequestID),
			Kind:  FactKindTrace,
			Value: s,
		})
	}
}
