package engines

import (
	"strings"
	"testing"
	"testing/fstest"
)

func validTestFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return fstest.MapFS{
		"data/jobs.yaml": &fstest.MapFile{
			Data: []byte(`- id: main_db
  label: Main database
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
`),
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
`)
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
`)
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
- job: main_db
  engine: postgresql
  fit: workable
  build: not_built
  reason: y
  fidelity: real
`)
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
`)
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
