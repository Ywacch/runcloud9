package templates

import (
	"regexp"
	"strings"
)

// NSSelectorPlaceholder is substituted with the Prometheus namespace label matcher at collect time.
const NSSelectorPlaceholder = "<<ns>>"

// PrometheusMetricQueryDef is one PromQL query published as templateMetrics[key].
type PrometheusMetricQueryDef struct {
	Key  string `yaml:"key"`
	Expr string `yaml:"expr"`
}

// PrometheusMetricAugmentDef copies a value from a collected component path into templateMetrics.
type PrometheusMetricAugmentDef struct {
	Key            string `yaml:"key"`
	Component      string `yaml:"component"`
	Path           string `yaml:"path"`
	OnlyIfPositive bool   `yaml:"onlyIfPositive,omitempty"`
}

// PrometheusMetricsSpec lists template-specific PromQL and optional component augments.
type PrometheusMetricsSpec struct {
	Queries  []PrometheusMetricQueryDef   `yaml:"queries"`
	Augments []PrometheusMetricAugmentDef `yaml:"augments,omitempty"`
}

var augmentPathPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*(\.[A-Za-z][A-Za-z0-9]*)*$`)

func validateMetricsProfile(id string, spec PrometheusMetricsSpec) {
	seen := make(map[string]struct{})
	for _, q := range spec.Queries {
		if q.Key == "" {
			panic("templates: metrics profile " + id + " has query with empty key")
		}
		if q.Expr == "" {
			panic("templates: metrics profile " + id + " query " + q.Key + " missing expr")
		}
		if !strings.Contains(q.Expr, NSSelectorPlaceholder) {
			panic("templates: metrics profile " + id + " query " + q.Key + " must contain " + NSSelectorPlaceholder)
		}
		if _, dup := seen[q.Key]; dup {
			panic("templates: metrics profile " + id + " duplicate query key " + q.Key)
		}
		seen[q.Key] = struct{}{}
	}
	for _, a := range spec.Augments {
		if a.Key == "" || a.Component == "" || a.Path == "" {
			panic("templates: metrics profile " + id + " augment missing key/component/path")
		}
		if !augmentPathPattern.MatchString(a.Path) {
			panic("templates: metrics profile " + id + " augment path must be dot.separated alnum identifiers: " + a.Path)
		}
	}
}

func resolvePrometheusMetrics(def *templateDef) *PrometheusMetricsSpec {
	if def.PrometheusMetricsRef != "" {
		panic("templates: prometheusMetricsRef is deprecated; move metrics into template YAML: " + def.ID)
	}
	if def.PrometheusMetrics != nil && (len(def.PrometheusMetrics.Queries) > 0 || len(def.PrometheusMetrics.Augments) > 0) {
		cp := *def.PrometheusMetrics
		validateMetricsProfile(def.ID, cp)
		return &cp
	}
	return nil
}
