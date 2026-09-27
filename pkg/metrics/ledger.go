package metrics

import (
	"fmt"
	"sort"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/templates"
)

// FactRef is a stable citation handle for one ledger entry.
// Formats:
//   - fact:config:{instanceID}.{settingKey}
//   - fact:metric:{component}.{signal}.last
//   - fact:metric:session.{peakKey}
//   - fact:trajectory:{metricKey}
//   - fact:event:{index}
//   - fact:rule:{ruleID}
//   - fact:violation:{ruleID}
//   - fact:wiring:{sourceNode}->{targetNode}.{field}
type FactRef string

// FactKind categorizes ledger entries for verifier checks.
const (
	FactKindConfig     = "config"
	FactKindMetric     = "metric"
	FactKindTrajectory = "trajectory"
	FactKindEvent      = "event"
	FactKindRule       = "rule"
	FactKindViolation  = "violation"
	FactKindDerived    = "derived"
	FactKindWiring     = "wiring"
)

// Fact is one citable object in a session fact ledger.
type Fact struct {
	Ref       FactRef          `json:"ref"`
	Kind      string           `json:"kind"`
	Component string           `json:"component,omitempty"`
	Value     any              `json:"value"`
	Freshness *SignalFreshness `json:"freshness,omitempty"`
	Timestamp *time.Time       `json:"timestamp,omitempty"`
}

// Ledger holds ordered facts plus a ref index for O(1) Resolve.
type Ledger struct {
	facts []Fact
	index map[FactRef]int
}

// LedgerInput feeds BuildLedger from session evidence projections.
type LedgerInput struct {
	DesignConfig deployer.DesignConfig
	Design       *deployer.DeploymentDesign
	Hist         []DeploymentMetrics
	Final        *DeploymentMetrics
	Template     *templates.TemplateSpec
}

// BuildLedger assembles config, metric, trajectory, event, rule, and violation
// facts from the session design and metrics evidence projections.
func BuildLedger(in LedgerInput) *Ledger {
	l := &Ledger{index: make(map[FactRef]int)}
	appendConfigFacts(l, in.DesignConfig)
	appendLoadgenMixFacts(l, in.DesignConfig.Template, in.Template)
	appendWiringFacts(l, in.Design)

	final := in.Final
	if final == nil && len(in.Hist) > 0 {
		final = &in.Hist[len(in.Hist)-1]
	}
	internals := BuildComponentInternals(final)
	descriptors := BuildDescriptors(internals)
	descriptorIndex := indexDescriptors(descriptors)

	appendMetricFacts(l, internals, descriptorIndex)
	appendDerivedCacheFacts(l, final, in.DesignConfig.Template)
	appendReplicaReadSplitFacts(l, final)
	appendSessionPeakFacts(l, PeakSummary(final))
	appendTrajectoryFacts(l, BuildTrajectories(in.Hist))

	violations := BuildViolations(in.Hist, in.DesignConfig, in.Template)
	appendViolationFacts(l, violations)
	appendRuleFacts(l, FiredRuleIDs(in.Hist, violations))
	appendEventFacts(l, in.Hist)

	return l
}

// Facts returns a copy of ledger facts in insertion order.
func (l *Ledger) Facts() []Fact {
	if l == nil || len(l.facts) == 0 {
		return nil
	}
	return append([]Fact(nil), l.facts...)
}

// Resolve returns the fact for ref when present.
func (l *Ledger) Resolve(ref FactRef) (Fact, bool) {
	if l == nil {
		return Fact{}, false
	}
	idx, ok := l.index[ref]
	if !ok {
		return Fact{}, false
	}
	return l.facts[idx], true
}

func (l *Ledger) addFact(f Fact) {
	if l == nil || f.Ref == "" {
		return
	}
	if _, exists := l.index[f.Ref]; exists {
		return
	}
	l.index[f.Ref] = len(l.facts)
	l.facts = append(l.facts, f)
}

func appendConfigFacts(l *Ledger, dc deployer.DesignConfig) {
	roles := sortedStrings(designRoleKeys(dc.Roles))
	for _, role := range roles {
		instances := append([]deployer.InstanceConfig(nil), dc.Roles[role]...)
		sort.Slice(instances, func(i, j int) bool {
			return instances[i].ID < instances[j].ID
		})
		for _, inst := range instances {
			keys := sortedStrings(keysOfAny(inst.Settings))
			for _, key := range keys {
				l.addFact(Fact{
					Ref:       FactRef(fmt.Sprintf("fact:config:%s.%s", inst.ID, key)),
					Kind:      FactKindConfig,
					Component: role,
					Value:     inst.Settings[key],
				})
			}
		}
	}
}

func appendMetricFacts(l *Ledger, internals ComponentInternals, descriptors map[string]SignalDescriptor) {
	roles := sortedStrings(componentRoleKeys(internals))
	for _, role := range roles {
		comp := internals[role]
		for _, inst := range comp.Instances {
			signals := sortedStrings(keysOfAny(inst.Signals))
			for _, signal := range signals {
				descKey := role + "\x00" + signal
				desc, hasDesc := descriptors[descKey]
				var fresh *SignalFreshness
				if hasDesc {
					fresh = freshnessPtr(desc.Freshness)
				}
				l.addFact(Fact{
					Ref:       FactRef(fmt.Sprintf("fact:metric:%s.%s.last", role, signal)),
					Kind:      FactKindMetric,
					Component: role,
					Value:     inst.Signals[signal],
					Freshness: fresh,
				})
			}
		}
	}
}

func appendSessionPeakFacts(l *Ledger, peaks map[string]any) {
	if len(peaks) == 0 {
		return
	}
	keys := sortedStrings(keysOfAny(peaks))
	for _, key := range keys {
		component := peakComponent(key)
		l.addFact(Fact{
			Ref:       FactRef("fact:metric:session." + key),
			Kind:      FactKindMetric,
			Component: component,
			Value:     peaks[key],
		})
	}
}

func appendTrajectoryFacts(l *Ledger, trajectories map[string]any) {
	if len(trajectories) == 0 {
		return
	}
	keys := sortedStrings(keysOfAny(trajectories))
	for _, key := range keys {
		component := trajectoryComponent(key)
		l.addFact(Fact{
			Ref:       FactRef("fact:trajectory:" + key),
			Kind:      FactKindTrajectory,
			Component: component,
			Value:     trajectories[key],
		})
	}
}

func appendViolationFacts(l *Ledger, violations []ViolationRollup) {
	for _, v := range violations {
		l.addFact(Fact{
			Ref:       FactRef("fact:violation:" + v.ID),
			Kind:      FactKindViolation,
			Component: violationComponent(v.ID),
			Value:     v,
		})
	}
}

func appendRuleFacts(l *Ledger, fired []string) {
	for _, ruleID := range fired {
		l.addFact(Fact{
			Ref:       FactRef("fact:rule:" + ruleID),
			Kind:      FactKindRule,
			Component: ruleComponent(ruleID),
			Value: map[string]any{
				"ruleId": ruleID,
				"fired":  true,
			},
		})
	}
}

func appendEventFacts(l *Ledger, hist []DeploymentMetrics) {
	idx := 0
	for _, snap := range hist {
		for _, ev := range snap.Events {
			ts := ev.Timestamp
			if ts.IsZero() {
				ts = snap.Timestamp
			}
			var tsPtr *time.Time
			if !ts.IsZero() {
				tsCopy := ts.UTC()
				tsPtr = &tsCopy
			}
			l.addFact(Fact{
				Ref:       FactRef(fmt.Sprintf("fact:event:%d", idx)),
				Kind:      FactKindEvent,
				Component: ev.Component,
				Value:     eventFactValue(ev),
				Timestamp: tsPtr,
			})
			idx++
		}
	}
}

func eventFactValue(ev Event) map[string]any {
	out := map[string]any{
		"type":   ev.Type,
		"source": ev.Source,
		"title":  ev.Title,
	}
	if ev.RuleID != "" {
		out["ruleId"] = ev.RuleID
	}
	if ev.Kind != "" {
		out["kind"] = ev.Kind
	}
	if len(ev.Detail) > 0 {
		out["detail"] = ev.Detail
	}
	if ev.Visibility != "" {
		out["visibility"] = ev.Visibility
	}
	return out
}

// FiredRuleIDs returns sorted unique rule IDs from session events and violations.
func FiredRuleIDs(hist []DeploymentMetrics, violations []ViolationRollup) []string {
	seen := make(map[string]bool)
	for _, snap := range hist {
		for _, ev := range snap.Events {
			if ev.RuleID != "" {
				seen[ev.RuleID] = true
			}
		}
	}
	for _, v := range violations {
		if v.ID != "" {
			seen[v.ID] = true
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func indexDescriptors(descs []SignalDescriptor) map[string]SignalDescriptor {
	if len(descs) == 0 {
		return nil
	}
	out := make(map[string]SignalDescriptor, len(descs))
	for _, d := range descs {
		out[d.Component+"\x00"+d.Name] = d
	}
	return out
}

func freshnessPtr(f SignalFreshness) *SignalFreshness {
	cp := f
	return &cp
}

func peakComponent(key string) string {
	switch key {
	case "peak_rps", "peak_p99_ms", "error_rate", "total_requests":
		return "app-server"
	case "total_cache_hits", "total_cache_misses", "cache_hit_rate":
		return "cache"
	case "db_query_rate", "db_total_queries":
		return "db"
	default:
		return ""
	}
}

func trajectoryComponent(key string) string {
	switch key {
	case "cache_hit_rate":
		return "cache"
	case "db_cpu_percent":
		return "db"
	case "app_p99_ms", "app_error_rate":
		return "app-server"
	default:
		return ""
	}
}

func violationComponent(ruleID string) string {
	if rule, ok := ruleLookup(ruleID, nil); ok {
		return rule.Component
	}
	return ""
}

func ruleComponent(ruleID string) string {
	return violationComponent(ruleID)
}

func sortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func designRoleKeys(m map[string][]deployer.InstanceConfig) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysOfAny(m map[string]any) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func componentRoleKeys(internals ComponentInternals) []string {
	if len(internals) == 0 {
		return nil
	}
	out := make([]string, 0, len(internals))
	for k := range internals {
		out = append(out, k)
	}
	return out
}
