package introspect

import "path/filepath"

// ComponentSpec defines what's browsable for a single component in a template.
type ComponentSpec struct {
	Engine      string   `json:"engine" yaml:"engine"`
	Tables      []string `json:"tables,omitempty" yaml:"tables"`
	KeyPatterns []string `json:"keyPatterns,omitempty" yaml:"keyPatterns"`
}

// Spec is the top-level introspection config from a template YAML.
type Spec map[string]ComponentSpec // keyed by component: "db", "cache", "mq"

// TableAllowed reports whether table is in the whitelist.
func TableAllowed(table string, tables []string) bool {
	for _, t := range tables {
		if t == table {
			return true
		}
	}
	return false
}

// KeyMatchesPatterns returns true if key matches any glob pattern.
func KeyMatchesPatterns(key string, patterns []string) bool {
	for _, p := range patterns {
		if matched, _ := filepath.Match(p, key); matched {
			return true
		}
	}
	return false
}

// PatternAllowed returns true if pattern is one of the configured key patterns.
func PatternAllowed(pattern string, patterns []string) bool {
	for _, p := range patterns {
		if p == pattern {
			return true
		}
	}
	return false
}
