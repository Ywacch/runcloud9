package metrics

import (
	"fmt"
	"sort"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

// ViolationRollup summarizes one violated rule for brain context and the fact
// ledger. Counts come from invariant events in session history, with
// ConsistencyStatus counters overriding when present on the final snapshot.
type ViolationRollup struct {
	ID       string            `json:"id"`
	Count    int64             `json:"count"`
	Detail   map[string]string `json:"detail,omitempty"`
	CausedBy []ConfigCause     `json:"causedBy,omitempty"`
}

// ConfigCause links a catalog causedByConfig key to the student's actual design.
type ConfigCause struct {
	Role     string `json:"role"`
	Instance string `json:"instance,omitempty"`
	Key      string `json:"key"`
	Value    any    `json:"value"`
}

type violationAcc struct {
	count  int64
	detail map[string]string
}

// BuildViolations rolls up invariant violations from session history and joins
// catalog causedByConfig keys to the session design_config values.
func BuildViolations(hist []DeploymentMetrics, dc deployer.DesignConfig, tmpl *templates.TemplateSpec) []ViolationRollup {
	byRule := make(map[string]violationAcc)
	for _, snap := range hist {
		for _, ev := range snap.Events {
			if ev.RuleID == "" || ev.Type != "invariant" {
				continue
			}
			acc := byRule[ev.RuleID]
			acc.count++
			if len(ev.Detail) > 0 {
				acc.detail = cloneDetail(ev.Detail)
			}
			byRule[ev.RuleID] = acc
		}
	}
	if len(hist) > 0 {
		mergeConsistencyViolationCounts(byRule, hist[len(hist)-1])
	}

	ids := make([]string, 0, len(byRule))
	for id, acc := range byRule {
		if acc.count <= 0 {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]ViolationRollup, 0, len(ids))
	for _, id := range ids {
		acc := byRule[id]
		rule, ok := ruleLookup(id, tmpl)
		v := ViolationRollup{
			ID:     id,
			Count:  acc.count,
			Detail: normalizeViolationDetail(acc.detail, acc.count),
		}
		if ok {
			v.CausedBy = resolveCausedBy(rule, dc)
		}
		out = append(out, v)
	}
	return out
}

func mergeConsistencyViolationCounts(byRule map[string]violationAcc, final DeploymentMetrics) {
	cons := consistencyStatusFromSnapshot(final)
	if cons == nil {
		return
	}
	for _, field := range consistencyViolationFields(cons) {
		if field.count <= 0 {
			continue
		}
		acc := byRule[field.id]
		acc.count = field.count
		if acc.detail == nil {
			acc.detail = map[string]string{}
		}
		if acc.detail["kind"] == "" && field.kind != "" {
			acc.detail["kind"] = field.kind
		}
		if acc.detail["value"] == "" {
			acc.detail["value"] = fmt.Sprintf("%d", field.count)
		}
		byRule[field.id] = acc
	}
}

func consistencyStatusFromSnapshot(dm DeploymentMetrics) *ConsistencyStatus {
	if db, ok := dm.Components["db"]; ok && db.Consistency != nil {
		return db.Consistency
	}
	for _, cm := range dm.Components {
		if cm.Consistency != nil {
			return cm.Consistency
		}
	}
	return nil
}

func consistencyViolationFields(cons *ConsistencyStatus) []struct {
	id    string
	count int64
	kind  string
} {
	return []struct {
		id    string
		count int64
		kind  string
	}{
		{rules.IDRYWViolation, cons.RYWViolations, "ryw"},
		{rules.IDMonotonicViolation, cons.MonotonicViolations, "monotonic"},
		{rules.IDCacheDBStale, cons.CacheDbStale, "cache_db_stale"},
		{rules.IDPartitionStaleReads, cons.PartitionStaleReads, "partition_stale_reads"},
		{rules.IDPartitionWriteStall, cons.PartitionWriteStall, "partition_write_stall"},
		{rules.IDReplicaCannotCatchUp, cons.ReplicaCannotCatchUp, "replica_cannot_catch_up"},
	}
}

func normalizeViolationDetail(detail map[string]string, count int64) map[string]string {
	if len(detail) == 0 && count <= 0 {
		return nil
	}
	out := cloneDetail(detail)
	if out == nil {
		out = map[string]string{}
	}
	if out["value"] == "" && count > 0 {
		out["value"] = fmt.Sprintf("%d", count)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneDetail(detail map[string]string) map[string]string {
	if len(detail) == 0 {
		return nil
	}
	out := make(map[string]string, len(detail))
	for k, v := range detail {
		out[k] = v
	}
	return out
}

func ruleLookup(id string, tmpl *templates.TemplateSpec) (rules.Rule, bool) {
	if r, ok := rules.Lookup(id); ok {
		return r, true
	}
	for _, r := range rules.CatalogFor(tmpl) {
		if r.ID == id {
			return r, true
		}
	}
	return rules.Rule{}, false
}

func resolveCausedBy(rule rules.Rule, dc deployer.DesignConfig) []ConfigCause {
	if len(rule.CausedByConfig) == 0 {
		return nil
	}
	roles := rolesForRule(rule, dc)
	var out []ConfigCause
	for _, role := range roles {
		for _, inst := range dc.Roles[role] {
			for _, key := range rule.CausedByConfig {
				value, ok := inst.Settings[key]
				if !ok {
					continue
				}
				out = append(out, ConfigCause{
					Role:     role,
					Instance: inst.ID,
					Key:      key,
					Value:    value,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Role != out[j].Role {
			return out[i].Role < out[j].Role
		}
		if out[i].Instance != out[j].Instance {
			return out[i].Instance < out[j].Instance
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func rolesForRule(rule rules.Rule, dc deployer.DesignConfig) []string {
	if rule.Component != "" {
		if len(dc.Roles[rule.Component]) == 0 {
			return nil
		}
		return []string{rule.Component}
	}
	roles := make([]string, 0, len(dc.Roles))
	for role := range dc.Roles {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	return roles
}
