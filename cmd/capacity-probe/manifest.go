package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Manifest describes a capacity campaign: ordered waves of concurrent deploys.
type Manifest struct {
	Name                  string `yaml:"name"`
	Tier                  string `yaml:"tier"`
	CooldownoldownSeconds int    `yaml:"cooldown_seconds"`
	Waves                 []Wave `yaml:"waves"`
}

// Wave is one concurrency experiment.
type Wave struct {
	Name   string      `yaml:"name"`
	Mode   string      `yaml:"mode"` // burst | paced
	PaceMS int         `yaml:"pace_ms"`
	Groups []WaveGroup `yaml:"groups"`
}

// WaveGroup is N copies of one design fixture.
type WaveGroup struct {
	Count  int    `yaml:"count"`
	Design string `yaml:"design"` // built-in name or path to JSON design file
}

func loadManifest(path string) (Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m Manifest
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("parse manifest: %w", err)
	}
	if err := validateManifest(m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

func validateManifest(m Manifest) error {
	if strings.TrimSpace(m.Name) == "" {
		return fmt.Errorf("manifest name is required")
	}
	tier := strings.ToLower(strings.TrimSpace(m.Tier))
	if tier == "" {
		return fmt.Errorf("manifest tier is required")
	}
	switch tier {
	case "explorer", "free", "engineer", "internal":
	default:
		return fmt.Errorf("unsupported tier %q", m.Tier)
	}
	if m.CooldownoldownSeconds < 0 {
		return fmt.Errorf("cooldown_seconds must be >= 0")
	}
	if len(m.Waves) == 0 {
		return fmt.Errorf("manifest must declare at least one wave")
	}
	for i, w := range m.Waves {
		if strings.TrimSpace(w.Name) == "" {
			return fmt.Errorf("waves[%d]: name is required", i)
		}
		mode := strings.ToLower(strings.TrimSpace(w.Mode))
		if mode == "" {
			mode = "burst"
		}
		switch mode {
		case "burst", "paced":
		default:
			return fmt.Errorf("waves[%d]: mode must be burst or paced", i)
		}
		if mode == "paced" && w.PaceMS < 0 {
			return fmt.Errorf("waves[%d]: pace_ms must be >= 0", i)
		}
		if len(w.Groups) == 0 {
			return fmt.Errorf("waves[%d]: at least one group is required", i)
		}
		total := 0
		for j, g := range w.Groups {
			if g.Count < 1 {
				return fmt.Errorf("waves[%d].groups[%d]: count must be >= 1", i, j)
			}
			if strings.TrimSpace(g.Design) == "" {
				return fmt.Errorf("waves[%d].groups[%d]: design is required", i, j)
			}
			total += g.Count
		}
		if total < 1 {
			return fmt.Errorf("waves[%d]: total deployments must be >= 1", i)
		}
	}
	return nil
}

func (m Manifest) cooldown() time.Duration {
	if m.CooldownoldownSeconds <= 0 {
		return 0
	}
	return time.Duration(m.CooldownoldownSeconds) * time.Second
}

func (m Manifest) effectiveTier() string {
	switch strings.ToLower(strings.TrimSpace(m.Tier)) {
	case "free":
		return "explorer"
	default:
		return strings.ToLower(strings.TrimSpace(m.Tier))
	}
}

func (w Wave) effectiveMode() string {
	mode := strings.ToLower(strings.TrimSpace(w.Mode))
	if mode == "" {
		return "burst"
	}
	return mode
}

func (w Wave) totalCount() int {
	n := 0
	for _, g := range w.Groups {
		n += g.Count
	}
	return n
}
