package deployer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

type deriveEnvGoldenEntry struct {
	Case   string            `json:"case"`
	NodeID string            `json:"nodeId"`
	Env    map[string]string `json:"env"`
}

func TestDeriveEnvVars_Characterization(t *testing.T) {
	t.Parallel()

	cases := deriveEnvCharacterizationCases()
	got := make([]deriveEnvGoldenEntry, 0)
	for _, tc := range cases {
		for _, n := range tc.design.Nodes {
			env := DeriveEnvVars(n, tc.design)
			got = append(got, deriveEnvGoldenEntry{
				Case:   tc.label,
				NodeID: n.ID,
				Env:    sortedEnvCopy(env),
			})
		}
	}

	sortDeriveEnvGoldenEntries(got)

	goldenPath := filepath.Join("testdata", "derive_env_golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		writeDeriveEnvGolden(t, goldenPath, got)
		return
	}

	want, err := readDeriveEnvGolden(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v (run with UPDATE_GOLDEN=1)", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("derive env golden mismatch: run UPDATE_GOLDEN=1 in pkg/deployer")
	}
}

func deriveEnvCharacterizationCases() []struct {
	label  string
	design DeploymentDesign
} {
	var out []struct {
		label  string
		design DeploymentDesign
	}
	for _, tmpl := range templates.Registry() {
		if !tmpl.Deployable {
			continue
		}
		if tmpl.Preset != nil {
			out = append(out, struct {
				label  string
				design DeploymentDesign
			}{
				label:  tmpl.ID + "/preset",
				design: deploymentDesignFromTemplatePreset(tmpl.ID, tmpl.Preset),
			})
		}
		if tmpl.PresetLite != nil {
			out = append(out, struct {
				label  string
				design DeploymentDesign
			}{
				label:  tmpl.ID + "/presetLite",
				design: deploymentDesignFromTemplatePreset(tmpl.ID, tmpl.PresetLite),
			})
		}
		for _, v := range tmpl.Variations {
			preset := &templates.TemplatePreset{Nodes: v.Nodes, Edges: v.Edges}
			out = append(out, struct {
				label  string
				design DeploymentDesign
			}{
				label:  tmpl.ID + "/variation/" + v.ID,
				design: deploymentDesignFromTemplatePreset(tmpl.ID, preset),
			})
		}
	}
	return out
}

func sortedEnvCopy(env map[string]string) map[string]string {
	if len(env) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		out[k] = v
	}
	return out
}

func writeDeriveEnvGolden(t *testing.T, path string, entries []deriveEnvGoldenEntry) {
	t.Helper()
	sortDeriveEnvGoldenEntries(entries)
	b, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readDeriveEnvGolden(path string) ([]deriveEnvGoldenEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []deriveEnvGoldenEntry
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func sortDeriveEnvGoldenEntries(entries []deriveEnvGoldenEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Case != entries[j].Case {
			return entries[i].Case < entries[j].Case
		}
		return entries[i].NodeID < entries[j].NodeID
	})
}

func TestJobEnv(t *testing.T) {
	t.Parallel()

	tmpl := templates.GetTemplate("social-feed")
	if tmpl == nil {
		t.Fatal("social-feed template missing")
	}
	design := deploymentDesignFromTemplatePreset("social-feed", tmpl.Preset)
	bindings := ResolveJobBindings(design, tmpl)

	app := nodeByID(design, "app-1")
	if app == nil {
		t.Fatal("app-1 missing")
	}
	env := jobEnv(*app, design, bindings)
	if env["MAIN_DB_ENGINE"] != "postgresql" || env["MAIN_DB_HOST"] != "db" || env["MAIN_DB_PORT"] != "5432" {
		t.Fatalf("app main_db env: %+v", env)
	}
	if env["TIMELINE_ENGINE"] != "redis" || env["TIMELINE_HOST"] != "cache" {
		t.Fatalf("app timeline env: %+v", env)
	}
	if env["WORK_QUEUE_ENGINE"] != "rabbitmq" || env["WORK_QUEUE_HOST"] != "mq" {
		t.Fatalf("app work_queue env (app->mq edge): %+v", env)
	}

	worker := nodeByID(design, "worker-1")
	if worker == nil {
		t.Fatal("worker-1 missing")
	}
	wenv := jobEnv(*worker, design, bindings)
	if wenv["WORK_QUEUE_ENGINE"] != "rabbitmq" || wenv["WORK_QUEUE_HOST"] != "mq" {
		t.Fatalf("worker work_queue env: %+v", wenv)
	}
	if wenv["TIMELINE_ENGINE"] != "redis" {
		t.Fatalf("worker timeline env: %+v", wenv)
	}

	dbNode := nodeByID(design, "db-1")
	if dbNode == nil {
		t.Fatal("db-1 missing")
	}
	if got := jobEnv(*dbNode, design, bindings); len(got) != 0 {
		t.Fatalf("db node should get no job env: %+v", got)
	}
}

func TestJobEnv_TimelineBoundToDB(t *testing.T) {
	t.Parallel()

	tmpl := templates.GetTemplate("social-feed")
	if tmpl == nil || tmpl.Preset == nil {
		t.Fatal("social-feed preset missing")
	}
	design := deploymentDesignFromTemplatePreset("social-feed", tmpl.Preset)
	design.JobBindings = map[string]string{"timeline": "db-1"}

	bindings := ResolveJobBindings(design, tmpl)
	app := nodeByID(design, "app-1")
	if app == nil {
		t.Fatal("app-1 missing")
	}
	env := jobEnv(*app, design, bindings)
	if env["TIMELINE_ENGINE"] != "postgresql" || env["TIMELINE_HOST"] != "db" || env["TIMELINE_PORT"] != "5432" {
		t.Fatalf("timeline on db: %+v", env)
	}
}

func TestDeriveEnvVars_IncludesJobEnv(t *testing.T) {
	t.Parallel()

	tmpl := templates.GetTemplate("url-shortener")
	if tmpl == nil || tmpl.Preset == nil {
		t.Fatal("url-shortener preset missing")
	}
	design := deploymentDesignFromTemplatePreset("url-shortener", tmpl.Preset)
	app := nodeByID(design, "app-1")
	if app == nil {
		t.Fatal("app-1 missing")
	}
	env := DeriveEnvVars(*app, design)
	for _, key := range []string{"MAIN_DB_ENGINE", "MAIN_DB_HOST", "MAIN_DB_PORT", "HOT_CACHE_ENGINE", "HOT_CACHE_HOST", "HOT_CACHE_PORT"} {
		if env[key] == "" {
			t.Fatalf("missing %s in derive env: %+v", key, env)
		}
	}
	if env["MAIN_DB_ENGINE"] != "postgresql" {
		t.Fatalf("MAIN_DB_ENGINE: %q", env["MAIN_DB_ENGINE"])
	}
	if env["HOT_CACHE_ENGINE"] != "redis" {
		t.Fatalf("HOT_CACHE_ENGINE: %q", env["HOT_CACHE_ENGINE"])
	}
	// Legacy vars unchanged
	if env["DB_HOST"] != "db" || env["CACHE_HOST"] != "cache" {
		t.Fatalf("legacy env: %+v", env)
	}
}
