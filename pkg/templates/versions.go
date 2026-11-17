package templates

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

var (
	imageVersionsMu sync.RWMutex
	imageVersions   map[string]string
)

// LoadVersionManifest loads image version overrides from a JSON manifest.
// Expected shape: { "image-name": "git-abc123" }.
func LoadVersionManifest(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read image versions file %q: %w", path, err)
	}
	parsed := make(map[string]string)
	if err := json.Unmarshal(data, &parsed); err != nil {
		return fmt.Errorf("parse image versions file %q: %w", path, err)
	}
	normalized := make(map[string]string, len(parsed))
	for name, tag := range parsed {
		n := strings.TrimSpace(name)
		t := strings.TrimSpace(tag)
		if n == "" || t == "" {
			continue
		}
		normalized[n] = t
	}
	imageVersionsMu.Lock()
	imageVersions = normalized
	imageVersionsMu.Unlock()
	return nil
}

// VersionManifest returns a copy of the loaded image tag map.
// The map is empty when no manifest has been loaded.
func VersionManifest() map[string]string {
	imageVersionsMu.RLock()
	defer imageVersionsMu.RUnlock()
	out := make(map[string]string, len(imageVersions))
	for name, tag := range imageVersions {
		out[name] = tag
	}
	return out
}

// ImageTag resolves an image tag with priority:
// 1) loaded manifest entry for image name
// 2) TEMPLATE_IMAGE_TAG env
// 3) hardcoded v1 fallback.
func ImageTag(name string) string {
	imageVersionsMu.RLock()
	if tag := imageVersions[name]; tag != "" {
		imageVersionsMu.RUnlock()
		return tag
	}
	imageVersionsMu.RUnlock()

	if tag := strings.TrimSpace(os.Getenv("TEMPLATE_IMAGE_TAG")); tag != "" {
		return tag
	}
	return "v1"
}
