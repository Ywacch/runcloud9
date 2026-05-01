package templates

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadVersionManifest_InvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatalf("write bad manifest: %v", err)
	}
	if err := LoadVersionManifest(path); err == nil {
		t.Fatal("expected parse error for invalid manifest")
	}
}

func TestImageTag_FallbackToEnvThenDefault(t *testing.T) {
	t.Setenv("TEMPLATE_IMAGE_TAG", "git-env")
	if got := ImageTag("unknown-template"); got != "git-env" {
		t.Fatalf("ImageTag env fallback: got %q want %q", got, "git-env")
	}
	t.Setenv("TEMPLATE_IMAGE_TAG", "")
	if got := ImageTag("unknown-template"); got != "v1" {
		t.Fatalf("ImageTag default fallback: got %q want %q", got, "v1")
	}
}
