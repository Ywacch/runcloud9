package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateManifest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		m       Manifest
		wantErr bool
	}{
		{
			name: "valid burst",
			m: Manifest{
				Name: "ok", Tier: "internal",
				Waves: []Wave{{Name: "w1", Mode: "burst", Groups: []WaveGroup{{Count: 5, Design: "url-shortener-small"}}}},
			},
		},
		{
			name: "missing name",
			m: Manifest{
				Tier:  "internal",
				Waves: []Wave{{Name: "w1", Groups: []WaveGroup{{Count: 1, Design: "url-shortener-small"}}}},
			},
			wantErr: true,
		},
		{
			name: "bad mode",
			m: Manifest{
				Name: "ok", Tier: "internal",
				Waves: []Wave{{Name: "w1", Mode: "swarm", Groups: []WaveGroup{{Count: 1, Design: "url-shortener-small"}}}},
			},
			wantErr: true,
		},
		{
			name: "zero count",
			m: Manifest{
				Name: "ok", Tier: "internal",
				Waves: []Wave{{Name: "w1", Groups: []WaveGroup{{Count: 0, Design: "url-shortener-small"}}}},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateManifest(tt.m)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadManifest_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "waves.yaml")
	content := `
name: fixture
tier: internal
cooldown_seconds: 30
waves:
  - name: wave-5
    mode: burst
    groups:
      - count: 5
        design: url-shortener-small
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "fixture" || m.effectiveTier() != "internal" || m.cooldown().Seconds() != 30 {
		t.Fatalf("unexpected manifest: %+v", m)
	}
	if m.Waves[0].totalCount() != 5 {
		t.Fatalf("totalCount=%d", m.Waves[0].totalCount())
	}
}
