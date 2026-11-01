package introspect

import (
	"path/filepath"
	"strings"
)

// ComponentSpec defines what's browsable for a single component in a template.
type ComponentSpec struct {
	Engine      string   `json:"engine" yaml:"engine"`
	Tables      []string `json:"tables,omitempty" yaml:"tables"`
	KeyPatterns []string `json:"keyPatterns,omitempty" yaml:"keyPatterns"`
	Queues      []string `json:"queues,omitempty" yaml:"queues"`
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

// QueueAllowed reports whether queue is in the whitelist.
func QueueAllowed(queue string, queues []string) bool {
	return TableAllowed(queue, queues)
}

// PatternHasGlob reports whether pattern uses Redis SCAN glob metacharacters.
func PatternHasGlob(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
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

// AppendQueueStreamKeys adds exact Redis stream keys for work queues that can
// share the cache instance. The stream key is the queue name (same string the
// RabbitMQ whitelist uses). patterns is not modified.
func AppendQueueStreamKeys(patterns, queues []string) []string {
	out := append([]string{}, patterns...)
	seen := make(map[string]struct{}, len(out)+len(queues))
	for _, p := range out {
		seen[p] = struct{}{}
	}
	for _, q := range queues {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if _, ok := seen[q]; ok {
			continue
		}
		seen[q] = struct{}{}
		out = append(out, q)
	}
	return out
}
