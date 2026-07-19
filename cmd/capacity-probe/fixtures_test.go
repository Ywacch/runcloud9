package main

import (
	"path/filepath"
	"testing"
)

func TestCheckedInWaveFixtures(t *testing.T) {
	t.Parallel()
	fixtures := []string{
		"homogeneous-url-shortener.yaml",
		"mixed-sizes.yaml",
		"paced-vs-burst.yaml",
	}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join("testdata", "waves", name)
			m, err := loadManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			baseDir := filepath.Dir(path)
			for _, wave := range m.Waves {
				rep, err := dryRunWave(wave, baseDir, m.effectiveTier())
				if err != nil {
					t.Fatalf("wave %s: %v", wave.Name, err)
				}
				if rep.RequestedCount < 1 || rep.RequestedWeight.CPUMillicores < 1 {
					t.Fatalf("wave %s: empty weight/count %+v", wave.Name, rep)
				}
			}
		})
	}
}
