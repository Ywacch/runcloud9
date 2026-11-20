package engines

import "fmt"

// Job need tokens (co-host eviction rules).
const (
	NeedEvicting    = "evicting"
	NeedNonEvicting = "non_evicting"
	NeedDurable     = "durable"
)

// Signal availability labels for pair manifests (M-1 closed list).
const (
	AvailabilityMeasured      = "measured"
	AvailabilityNotApplicable = "not_applicable"
	AvailabilityUnavailable   = "unavailable"
	AvailabilityNotBuilt      = "not_built"
)

// Signal concept labels (plan section 5.4).
const (
	ConceptCacheEfficiency = "cache_efficiency"
	ConceptConsumerLag     = "consumer_lag"
	ConceptContention      = "contention"
	ConceptDurabilitySync  = "durability_sync"
	ConceptEviction        = "eviction"
	ConceptFlowControl     = "flow_control"
	ConceptMemoryOverhead  = "memory_overhead"
	ConceptRedelivery      = "redelivery"
	ConceptServerLatency   = "server_latency"
)

// Introspect capabilities exposed in the pair manifest.
const (
	IntrospectTables     = "tables"
	IntrospectKeys       = "keys"
	IntrospectQueues     = "queues"
	IntrospectStreamPeek = "stream_peek"
)

var (
	validJobNeeds = map[string]struct{}{
		NeedEvicting:    {},
		NeedNonEvicting: {},
		NeedDurable:     {},
	}
	validAvailability = map[string]struct{}{
		AvailabilityMeasured:      {},
		AvailabilityNotApplicable: {},
		AvailabilityUnavailable:   {},
		AvailabilityNotBuilt:      {},
	}
	validConcept = map[string]struct{}{
		ConceptCacheEfficiency: {},
		ConceptConsumerLag:     {},
		ConceptContention:      {},
		ConceptDurabilitySync:  {},
		ConceptEviction:        {},
		ConceptFlowControl:     {},
		ConceptMemoryOverhead:  {},
		ConceptRedelivery:      {},
		ConceptServerLatency:   {},
	}
	validIntrospect = map[string]struct{}{
		IntrospectTables:     {},
		IntrospectKeys:       {},
		IntrospectQueues:     {},
		IntrospectStreamPeek: {},
	}
)

// ValidatePairSignalMeta rejects unknown availability or concept on a signal row.
func ValidatePairSignalMeta(s PairSignal) error {
	if s.Field == "" {
		return fmt.Errorf("empty field")
	}
	if s.Availability != "" {
		if _, ok := validAvailability[s.Availability]; !ok {
			return fmt.Errorf("bad availability %q", s.Availability)
		}
	}
	if s.Concept != "" {
		if _, ok := validConcept[s.Concept]; !ok {
			return fmt.Errorf("bad concept %q", s.Concept)
		}
	}
	return nil
}
