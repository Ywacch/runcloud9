package manifest

import (
	"encoding/json"
	"fmt"
)

// DecodeSummary unmarshals run_manifest from a metrics_summary value.
func DecodeSummary(raw any) (RunManifest, error) {
	if raw == nil {
		return RunManifest{}, fmt.Errorf("manifest: missing run_manifest")
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return RunManifest{}, fmt.Errorf("manifest: marshal summary: %w", err)
	}
	var m RunManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return RunManifest{}, fmt.Errorf("manifest: unmarshal run_manifest: %w", err)
	}
	return m, nil
}

// EncodeSummary marshals a manifest for metrics_summary.run_manifest.
func EncodeSummary(m RunManifest) (map[string]any, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}
