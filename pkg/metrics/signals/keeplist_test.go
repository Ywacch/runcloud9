package signals

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestKeepList_CoversCatalog(t *testing.T) {
	t.Parallel()
	keep := KeepList()
	keepSet := make(map[string]struct{}, len(keep))
	for _, m := range keep {
		keepSet[m] = struct{}{}
	}
	for _, s := range Catalog() {
		for _, m := range s.Metrics {
			if _, ok := keepSet[m]; !ok {
				t.Fatalf("signal %q metric %q missing from KeepList", s.ID, m)
			}
		}
	}
	if !slices.IsSorted(keep) {
		t.Fatal("KeepList must be sorted")
	}
	if len(keep) != len(keepSet) {
		t.Fatal("KeepList contains duplicates")
	}
}

func TestPrometheusValues_MatchesKeepList(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "..", "k8s", "helm", "prometheus-values.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	gotRegex := prometheusValuesKeepRegex(text)
	wantRegex := PrometheusKeepRegex()
	if gotRegex != wantRegex {
		t.Fatalf("prometheus keep regex out of sync\nrun: go test ./pkg/metrics/signals/ -run TestPrometheusValues_MatchesKeepList -count=1\nwant length %d got %d", len(wantRegex), len(gotRegex))
	}
	for _, m := range KeepList() {
		if !strings.Contains(text, m) {
			t.Fatalf("prometheus-values.yaml missing catalog metric %q", m)
		}
	}
	if !strings.Contains(text, ControlPlaneMetricsPrefix) {
		t.Fatal("prometheus-values.yaml must keep control-plane cloud9_ metrics")
	}
	if !strings.Contains(text, "retention: 2h") {
		t.Fatal("prometheus-values.yaml must set retention: 2h")
	}
	for _, needle := range []string{"storage.tsdb.min-block-duration", "30m"} {
		if !strings.Contains(text, needle) {
			t.Fatalf("prometheus-values.yaml missing %q", needle)
		}
	}
}

var prometheusKeepRegexLine = regexp.MustCompile(`(?m)^\s+regex:\s*['"]?(.+?)['"]?\s*$`)

func prometheusValuesKeepRegex(yaml string) string {
	inRelabel := false
	for _, line := range strings.Split(yaml, "\n") {
		trim := strings.TrimSpace(line)
		if strings.Contains(trim, "metric_relabel_configs:") {
			inRelabel = true
			continue
		}
		if inRelabel && strings.HasPrefix(trim, "action: keep") {
			// regex should appear on the preceding line in our file layout
		}
		if m := prometheusKeepRegexLine.FindStringSubmatch(line); m != nil && strings.Contains(yaml, "action: keep") {
			if strings.HasPrefix(m[1], "^(?:") {
				return m[1]
			}
		}
	}
	// Fallback: first catalog-style keep regex in file.
	for _, m := range prometheusKeepRegexLine.FindAllStringSubmatch(yaml, -1) {
		if strings.HasPrefix(m[1], "^(?:") {
			return m[1]
		}
	}
	return ""
}
