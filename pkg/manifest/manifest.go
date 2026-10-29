// Package manifest builds run manifests that pin what a deployment executed.
package manifest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

// SchemaVersion is bumped whenever RunManifest's JSON shape changes.
const SchemaVersion = 1

// RunManifest pins what a run executed, so a report can be reproduced.
type RunManifest struct {
	SchemaVersion      int               `json:"schemaVersion"`
	TemplateID         string            `json:"templateId"`
	TemplateVersion    string            `json:"templateVersion"`
	ImageVersions      map[string]string `json:"imageVersions"`
	SeedRef            string            `json:"seedRef,omitempty"`
	LoadProfile        *json.RawMessage  `json:"loadProfile,omitempty"`
	LoadProfileChanges int               `json:"loadProfileChanges,omitempty"`
	RNGSeed            int64             `json:"rngSeed"`
	DesignHash         string            `json:"designHash"`
	JobBindings        map[string]string `json:"jobBindings,omitempty"`
	EngineTuning       map[string]any    `json:"engineTuning,omitempty"`
	Instrumentation    map[string]string `json:"instrumentation,omitempty"`
	CreatedAt          time.Time         `json:"createdAt"`
}

// Input is everything Build needs. Pure data.
type Input struct {
	TemplateID, TemplateVersion string
	ImageVersions               map[string]string
	SeedRef                     string
	Design                      deployer.DeploymentDesign
	RNGSeed                     int64
	Now                         time.Time
}

// Build assembles a run manifest from pinned inputs.
func Build(in Input) (RunManifest, error) {
	if in.TemplateID == "" {
		return RunManifest{}, fmt.Errorf("manifest: template id required")
	}
	designHash, err := DesignHash(in.Design)
	if err != nil {
		return RunManifest{}, fmt.Errorf("manifest: design hash: %w", err)
	}
	if in.RNGSeed <= 0 {
		return RunManifest{}, fmt.Errorf("manifest: rng seed must be positive")
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	imgs := map[string]string{}
	for k, v := range in.ImageVersions {
		if v != "" {
			imgs[k] = v
		}
	}
	return RunManifest{
		SchemaVersion:   SchemaVersion,
		TemplateID:      in.TemplateID,
		TemplateVersion: in.TemplateVersion,
		ImageVersions:   imgs,
		SeedRef:         in.SeedRef,
		RNGSeed:         in.RNGSeed,
		DesignHash:      designHash,
		CreatedAt:       now.UTC(),
	}, nil
}
