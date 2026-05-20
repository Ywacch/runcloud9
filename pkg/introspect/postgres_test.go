package introspect

import (
	"slices"
	"testing"
)

func TestPostgresCapabilities(t *testing.T) {
	t.Parallel()
	d := &PostgresDriver{}
	caps := d.Capabilities()
	for _, want := range []Capability{CapTables, CapSchema, CapRows, CapIndexes} {
		if !slices.Contains(caps, want) {
			t.Fatalf("capabilities %v missing %q", caps, want)
		}
	}
}

func TestPageOptsClamp(t *testing.T) {
	t.Parallel()
	opts := PageOpts{Page: 1, Limit: 500}
	clamped := ClampPageOpts(opts)
	if clamped.Limit != 100 {
		t.Fatalf("expected limit 100, got %d", clamped.Limit)
	}
	if clamped.Page != 1 {
		t.Fatalf("expected page 1, got %d", clamped.Page)
	}
}

func TestPageOptsClampDefaults(t *testing.T) {
	t.Parallel()
	clamped := ClampPageOpts(PageOpts{})
	if clamped.Limit != 50 {
		t.Fatalf("expected default limit 50, got %d", clamped.Limit)
	}
	if clamped.Order != "asc" {
		t.Fatalf("expected asc order, got %q", clamped.Order)
	}
}
