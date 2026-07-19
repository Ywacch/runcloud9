package main

import "testing"

func TestComputeWeight_SmallVsLarge(t *testing.T) {
	t.Parallel()
	small := computeWeight(urlShortenerSmall(), "internal")
	large := computeWeight(urlShortenerLarge(), "internal")
	if small.CPUMillicores <= 0 || small.MemoryMi <= 0 {
		t.Fatalf("small weight invalid: %+v", small)
	}
	if large.CPUMillicores <= small.CPUMillicores {
		t.Fatalf("expected large cpu > small: large=%d small=%d", large.CPUMillicores, small.CPUMillicores)
	}
	if large.MemoryMi <= small.MemoryMi {
		t.Fatalf("expected large mem > small: large=%d small=%d", large.MemoryMi, small.MemoryMi)
	}
}

func TestScaleAndAddWeight(t *testing.T) {
	t.Parallel()
	w := DeploymentWeight{CPUMillicores: 100, MemoryMi: 256}
	got := scaleWeight(w, 5)
	if got.CPUMillicores != 500 || got.MemoryMi != 1280 {
		t.Fatalf("scaleWeight: %+v", got)
	}
	sum := addWeight(w, got)
	if sum.CPUMillicores != 600 || sum.MemoryMi != 1536 {
		t.Fatalf("addWeight: %+v", sum)
	}
}
