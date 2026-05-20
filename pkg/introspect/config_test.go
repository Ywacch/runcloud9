package introspect

import "testing"

func TestKeyMatchesPatterns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key      string
		patterns []string
		want     bool
	}{
		{"short:abc123", []string{"short:*"}, true},
		{"timeline:42", []string{"short:*"}, false},
		{"timeline:42", []string{"short:*", "timeline:*"}, true},
		{"other:key", []string{"short:*"}, false},
	}
	for _, tt := range tests {
		got := KeyMatchesPatterns(tt.key, tt.patterns)
		if got != tt.want {
			t.Errorf("KeyMatchesPatterns(%q, %v) = %v, want %v", tt.key, tt.patterns, got, tt.want)
		}
	}
}

func TestTableAllowed(t *testing.T) {
	t.Parallel()
	if !TableAllowed("urls", []string{"urls"}) {
		t.Fatal("expected urls allowed")
	}
	if TableAllowed("pg_catalog", []string{"urls"}) {
		t.Fatal("expected pg_catalog rejected")
	}
}
