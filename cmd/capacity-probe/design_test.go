package main

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestBuiltinDesignsAssessFull(t *testing.T) {
	t.Parallel()
	for _, name := range listBuiltinDesigns() {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			d, ok := builtinDesign(name)
			if !ok {
				t.Fatal("missing builtin")
			}
			if err := deployer.ValidateTopology(d); err != nil {
				t.Fatalf("ValidateTopology: %v", err)
			}
			a := deployer.AssessTopology(d)
			if a.Verdict != deployer.VerdictFull {
				t.Fatalf("verdict=%q explanation=%s", a.Verdict, a.Explanation)
			}
		})
	}
}

func TestResolveDesign_BuiltinAndFile(t *testing.T) {
	t.Parallel()
	d, err := resolveDesign("url-shortener-small", ".")
	if err != nil {
		t.Fatal(err)
	}
	if d.Template != "url-shortener" {
		t.Fatalf("template=%q", d.Template)
	}
	if _, err := resolveDesign("does-not-exist", t.TempDir()); err == nil {
		t.Fatal("expected error for missing design")
	}
}
