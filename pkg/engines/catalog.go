package engines

import (
	"embed"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var jobComponentRE = regexp.MustCompile(`^[a-z][a-z-]*$`)

//go:embed data/jobs.yaml data/engines.yaml data/pairs.yaml data/guarantees/*.yaml
var embeddedData embed.FS

var (
	defaultOnce sync.Once
	defaultCat  *Catalog
)

// Default returns the built-in catalog. Callers must not mutate it.
func Default() *Catalog {
	defaultOnce.Do(func() {
		cat, err := load(embeddedData)
		if err != nil {
			panic("engines: invalid default catalog: " + err.Error())
		}
		defaultCat = cat
	})
	return defaultCat
}

// Pair returns the entry for job and engine, or false.
func (c *Catalog) Pair(job, engine string) (Pair, bool) {
	for _, p := range c.Pairs {
		if p.Job == job && p.Engine == engine {
			return p, true
		}
	}
	return Pair{}, false
}

// JobByID returns the job entry, or false.
func (c *Catalog) JobByID(id string) (Job, bool) {
	for _, j := range c.Jobs {
		if j.ID == id {
			return j, true
		}
	}
	return Job{}, false
}

// EngineByID returns the engine entry, or false.
func (c *Catalog) EngineByID(id string) (Engine, bool) {
	for _, e := range c.Engines {
		if e.ID == id {
			return e, true
		}
	}
	return Engine{}, false
}

// SupportsReplicaApplyDelay reports whether the catalog lists lag-dial support for engine id.
func SupportsReplicaApplyDelay(engineID string) bool {
	e, ok := Default().EngineByID(strings.ToLower(strings.TrimSpace(engineID)))
	if !ok && engineID == "" {
		e, ok = Default().EngineByID("postgresql")
	}
	return ok && e.ReplicaApplyDelay
}

func load(fsys fs.FS) (*Catalog, error) {
	jobs, err := readYAML[[]Job](fsys, "data/jobs.yaml")
	if err != nil {
		return nil, err
	}
	engines, err := readYAML[[]Engine](fsys, "data/engines.yaml")
	if err != nil {
		return nil, err
	}
	pairs, err := readYAML[[]Pair](fsys, "data/pairs.yaml")
	if err != nil {
		return nil, err
	}
	guarantees, err := loadGuarantees(fsys)
	if err != nil {
		return nil, err
	}

	c := &Catalog{
		Version:    CatalogVersion,
		Jobs:       jobs,
		Engines:    engines,
		Pairs:      pairs,
		Guarantees: guarantees,
	}
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func readYAML[T any](fsys fs.FS, name string) (T, error) {
	var out T
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return out, fmt.Errorf("engines: read %s: %w", name, err)
	}
	if err := yaml.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("engines: parse %s: %w", name, err)
	}
	return out, nil
}

func loadGuarantees(fsys fs.FS) ([]GuaranteesCard, error) {
	dir := "data/guarantees"
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("engines: read guarantees dir: %w", err)
	}
	var out []GuaranteesCard
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := path.Join(dir, e.Name())
		card, err := readYAML[GuaranteesCard](fsys, name)
		if err != nil {
			return nil, err
		}
		out = append(out, card)
	}
	return out, nil
}

func validate(c *Catalog) error {
	if c == nil {
		return fmt.Errorf("engines: nil catalog")
	}
	if c.Version != CatalogVersion {
		return fmt.Errorf("engines: catalog version %q", c.Version)
	}

	jobIDs := make(map[string]struct{}, len(c.Jobs))
	for _, j := range c.Jobs {
		if j.ID == "" {
			return fmt.Errorf("engines: job with empty id")
		}
		if _, dup := jobIDs[j.ID]; dup {
			return fmt.Errorf("engines: duplicate job id %q", j.ID)
		}
		jobIDs[j.ID] = struct{}{}
		if strings.TrimSpace(j.Component) == "" {
			return fmt.Errorf("engines: job %q: missing component", j.ID)
		}
		if !jobComponentRE.MatchString(j.Component) {
			return fmt.Errorf("engines: job %q: bad component %q", j.ID, j.Component)
		}
		for _, need := range j.Needs {
			if _, ok := validJobNeeds[need]; !ok {
				return fmt.Errorf("engines: job %q: unknown need %q", j.ID, need)
			}
		}
	}

	engineIDs := make(map[string]struct{}, len(c.Engines))
	for _, e := range c.Engines {
		if e.ID == "" {
			return fmt.Errorf("engines: engine with empty id")
		}
		if _, dup := engineIDs[e.ID]; dup {
			return fmt.Errorf("engines: duplicate engine id %q", e.ID)
		}
		engineIDs[e.ID] = struct{}{}
	}

	seenPairs := make(map[string]struct{}, len(c.Pairs))
	for _, p := range c.Pairs {
		key := p.Job + "\x00" + p.Engine
		if _, dup := seenPairs[key]; dup {
			return fmt.Errorf("engines: duplicate pair %s/%s", p.Job, p.Engine)
		}
		seenPairs[key] = struct{}{}

		if _, ok := jobIDs[p.Job]; !ok {
			return fmt.Errorf("engines: pair references unknown job %q", p.Job)
		}
		if _, ok := engineIDs[p.Engine]; !ok {
			return fmt.Errorf("engines: pair references unknown engine %q", p.Engine)
		}
		if err := checkFit(p.Fit); err != nil {
			return fmt.Errorf("engines: pair %s/%s: %w", p.Job, p.Engine, err)
		}
		if err := checkBuild(p.Build); err != nil {
			return fmt.Errorf("engines: pair %s/%s: %w", p.Job, p.Engine, err)
		}
		if err := checkFidelity(p.Fidelity); err != nil {
			return fmt.Errorf("engines: pair %s/%s: %w", p.Job, p.Engine, err)
		}
		if p.Fit == FitPoor && p.Demo == nil {
			return fmt.Errorf("engines: pair %s/%s: poor fit requires failureDemo", p.Job, p.Engine)
		}
		if p.Introspect == "" {
			return fmt.Errorf("engines: pair %s/%s: missing introspect", p.Job, p.Engine)
		}
		if _, ok := validIntrospect[p.Introspect]; !ok {
			return fmt.Errorf("engines: pair %s/%s: bad introspect %q", p.Job, p.Engine, p.Introspect)
		}
		if len(p.Chaos) == 0 {
			return fmt.Errorf("engines: pair %s/%s: missing chaos list", p.Job, p.Engine)
		}
		for _, s := range p.Signals {
			if err := ValidatePairSignalMeta(s); err != nil {
				return fmt.Errorf("engines: pair %s/%s signal %q: %w", p.Job, p.Engine, s.Field, err)
			}
		}
	}

	for _, card := range c.Guarantees {
		if _, ok := jobIDs[card.Job]; !ok {
			return fmt.Errorf("engines: guarantees reference unknown job %q", card.Job)
		}
		if _, ok := engineIDs[card.Engine]; !ok {
			return fmt.Errorf("engines: guarantees reference unknown engine %q", card.Engine)
		}
		for _, e := range card.Entries {
			if err := checkFidelity(e.Fidelity); err != nil {
				return fmt.Errorf("engines: guarantee %q: %w", e.ID, err)
			}
			if e.Status != "verified" && e.Status != "unverified" {
				return fmt.Errorf("engines: guarantee %q: bad status %q", e.ID, e.Status)
			}
			if e.Status == "verified" && (e.Chaos == "" || e.Check == "") {
				return fmt.Errorf("engines: guarantee %q: verified requires chaos and check", e.ID)
			}
			if e.PACELC != "" && e.PACELC != "partition" && e.PACELC != "else" {
				return fmt.Errorf("engines: guarantee %q: bad pacelc %q", e.ID, e.PACELC)
			}
		}
	}

	return nil
}

func checkFit(f Fit) error {
	switch f {
	case FitNatural, FitWorkable, FitPoor, FitImpossible:
		return nil
	default:
		return fmt.Errorf("bad fit %q", f)
	}
}

func checkBuild(b Build) error {
	switch b {
	case BuildAvailable, BuildNotBuilt, BuildPlanned:
		return nil
	default:
		return fmt.Errorf("bad build %q", b)
	}
}

func checkFidelity(s string) error {
	switch s {
	case FidelityReal, FidelitySimulated, FidelityCompressed:
		return nil
	default:
		return fmt.Errorf("bad fidelity %q", s)
	}
}
