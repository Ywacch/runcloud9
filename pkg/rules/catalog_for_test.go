package rules

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestCatalogFor_NilTemplateReturnsStatic(t *testing.T) {
	t.Parallel()
	static := All()
	got := CatalogFor(nil)
	if len(got) != len(static) {
		t.Fatalf("len(CatalogFor(nil)) = %d, want %d", len(got), len(static))
	}
	for i := range static {
		if got[i].ID != static[i].ID {
			t.Fatalf("rule[%d].ID = %q, want %q", i, got[i].ID, static[i].ID)
		}
		if got[i].Family != static[i].Family {
			t.Fatalf("rule[%d].Family = %q, want %q", i, got[i].Family, static[i].Family)
		}
	}
}

func TestCatalogFor_AddsTemplateInvariants(t *testing.T) {
	t.Parallel()
	tmpl := &templates.TemplateSpec{
		InvariantCheckers: []templates.InvariantRule{{
			ID:    "oversell",
			Kind:  "oversell",
			Title: "Oversell detected",
		}},
	}
	got := CatalogFor(tmpl)
	if len(got) != 21 {
		t.Fatalf("len(CatalogFor) = %d, want 21", len(got))
	}
	inv := got[len(got)-1]
	if inv.ID != "oversell" {
		t.Fatalf("overlay ID = %q, want oversell", inv.ID)
	}
	if inv.Family != FamilyInvariant {
		t.Fatalf("overlay Family = %q, want %q", inv.Family, FamilyInvariant)
	}
	if inv.Kind != "oversell" {
		t.Fatalf("overlay Kind = %q, want oversell", inv.Kind)
	}
	if inv.Summary != "Oversell detected" {
		t.Fatalf("overlay Summary = %q, want Oversell detected", inv.Summary)
	}
}
