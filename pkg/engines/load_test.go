package engines

import (
	"strings"
	"testing"
	"testing/fstest"
)

// pairMetaYAML is the minimum manifest fields E-23b validation requires on every pair.
const pairMetaYAML = `  introspect: tables
  chaos:
    - pod_kill
`

func validTestFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return fstest.MapFS{
		"data/jobs.yaml": &fstest.MapFile{
			Data: []byte(`- id: main_db
  label: Main database
  component: db
`),
		},
		"data/engines.yaml": &fstest.MapFile{
			Data: []byte(`- id: postgresql
  label: PostgreSQL
  family: relational
  weight: light
  minShape: {pods: 1, cpuMilli: 125, memoryMi: 256}
`),
		},
		"data/pairs.yaml": &fstest.MapFile{
			Data: []byte(`- job: main_db
  engine: postgresql
  fit: natural
  build: available
  reason: ok
  fidelity: real
` + pairMetaYAML),
		},
		"data/guarantees/empty.yaml": &fstest.MapFile{
			Data: []byte(`job: main_db
engine: postgresql
entries: []
`),
		},
	}
}

func TestLoad_RejectsUnknownJob(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	pairs, _ := fsys["data/pairs.yaml"]
	pairs.Data = []byte(`- job: unknown_job
  engine: postgresql
  fit: natural
  build: available
  reason: x
  fidelity: real
` + pairMetaYAML)
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "unknown job") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoad_RejectsUnknownEngine(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	pairs, _ := fsys["data/pairs.yaml"]
	pairs.Data = []byte(`- job: main_db
  engine: unknown_engine
  fit: natural
  build: available
  reason: x
  fidelity: real
` + pairMetaYAML)
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "unknown engine") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoad_RejectsDuplicatePair(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	pairs, _ := fsys["data/pairs.yaml"]
	pairs.Data = []byte(`- job: main_db
  engine: postgresql
  fit: natural
  build: available
  reason: x
  fidelity: real
` + pairMetaYAML + `- job: main_db
  engine: postgresql
  fit: workable
  build: not_built
  reason: y
  fidelity: real
` + pairMetaYAML)
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "duplicate pair") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoad_RejectsPoorWithoutDemo(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	pairs, _ := fsys["data/pairs.yaml"]
	pairs.Data = []byte(`- job: main_db
  engine: postgresql
  fit: poor
  build: not_built
  reason: x
  fidelity: real
` + pairMetaYAML)
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "failureDemo") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoad_RejectsBadYAML(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	jobs, _ := fsys["data/jobs.yaml"]
	jobs.Data = []byte(`[{ not valid yaml`)
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoad_MinimalFS(t *testing.T) {
	t.Parallel()
	c, err := load(validTestFS(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Jobs) != 1 || len(c.Engines) != 1 || len(c.Pairs) != 1 {
		t.Fatalf("catalog: %+v", c)
	}
}

func guaranteeCard(t *testing.T, c *Catalog, job, engine string) GuaranteesCard {
	t.Helper()
	for _, card := range c.Guarantees {
		if card.Job == job && card.Engine == engine {
			return card
		}
	}
	t.Fatalf("missing guarantees card %s/%s", job, engine)
	return GuaranteesCard{}
}

func guaranteeEntryIDs(t *testing.T, card GuaranteesCard) map[string]GuaranteeEntry {
	t.Helper()
	out := make(map[string]GuaranteeEntry, len(card.Entries))
	for _, e := range card.Entries {
		if _, dup := out[e.ID]; dup {
			t.Fatalf("duplicate guarantee id %q on %s/%s", e.ID, card.Job, card.Engine)
		}
		out[e.ID] = e
	}
	return out
}

func TestDefault_TimelineGuaranteeCards(t *testing.T) {
	t.Parallel()
	c := Default()
	if len(c.Guarantees) != 2 {
		t.Fatalf("guarantees cards: want 2, got %d", len(c.Guarantees))
	}

	redis := guaranteeEntryIDs(t, guaranteeCard(t, c, "timeline", "redis"))
	wantRedis := []string{"timeline_lost_on_cache_restart", "timeline_evicted_under_memory_pressure"}
	for _, id := range wantRedis {
		e, ok := redis[id]
		if !ok {
			t.Fatalf("redis/timeline missing entry %q", id)
		}
		if e.Status != "unverified" {
			t.Fatalf("%s: want unverified, got %q", id, e.Status)
		}
		if e.Fidelity != FidelityReal {
			t.Fatalf("%s: fidelity %q", id, e.Fidelity)
		}
	}
	if redis["timeline_lost_on_cache_restart"].Chaos != "pod_kill" {
		t.Fatalf("cache restart chaos: %q", redis["timeline_lost_on_cache_restart"].Chaos)
	}
	if redis["timeline_lost_on_cache_restart"].Check != "social_timeline_keys" {
		t.Fatalf("cache restart check: %q", redis["timeline_lost_on_cache_restart"].Check)
	}
	if redis["timeline_evicted_under_memory_pressure"].Chaos != "cache_flush" {
		t.Fatalf("eviction chaos: %q", redis["timeline_evicted_under_memory_pressure"].Chaos)
	}

	pg := guaranteeEntryIDs(t, guaranteeCard(t, c, "timeline", "postgresql"))
	wantPG := []string{"timeline_survives_db_restart", "timeline_trim_dead_tuples"}
	for _, id := range wantPG {
		e, ok := pg[id]
		if !ok {
			t.Fatalf("postgresql/timeline missing entry %q", id)
		}
		if e.Status != "unverified" {
			t.Fatalf("%s: want unverified, got %q", id, e.Status)
		}
	}
	if pg["timeline_survives_db_restart"].Chaos != "pod_kill" {
		t.Fatalf("db restart chaos: %q", pg["timeline_survives_db_restart"].Chaos)
	}
	if pg["timeline_trim_dead_tuples"].Chaos != "" {
		t.Fatalf("dead tuples entry should be check-only chaos, got %q", pg["timeline_trim_dead_tuples"].Chaos)
	}
	if pg["timeline_trim_dead_tuples"].Check != "timeline_dead_tuples" {
		t.Fatalf("dead tuples check: %q", pg["timeline_trim_dead_tuples"].Check)
	}
}

func TestLoad_RejectsVerifiedWithoutChaosCheck(t *testing.T) {
	t.Parallel()
	fsys := validTestFS(t)
	fsys["data/guarantees/bad.yaml"] = &fstest.MapFile{
		Data: []byte(`job: main_db
engine: postgresql
entries:
  - id: bad
    claim: x
    status: verified
    fidelity: real
`),
	}
	_, err := load(fsys)
	if err == nil || !strings.Contains(err.Error(), "verified requires chaos and check") {
		t.Fatalf("err = %v", err)
	}
}
