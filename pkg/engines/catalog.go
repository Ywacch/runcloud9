package engines

import (
	"fmt"
	"sync"
)

var (
	defaultOnce sync.Once
	defaultCat  *Catalog
)

// Default returns the built-in catalog. Callers must not mutate it.
func Default() *Catalog {
	defaultOnce.Do(func() {
		defaultCat = buildDefaultCatalog()
		if err := validate(defaultCat); err != nil {
			panic("engines: invalid default catalog: " + err.Error())
		}
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

func buildDefaultCatalog() *Catalog {
	return &Catalog{
		Version: CatalogVersion,
		Jobs: []Job{
			{ID: "main_db", Label: "Main database"},
			{ID: "hot_cache", Label: "Hot cache"},
			{
				ID:    "timeline",
				Label: "Timeline",
				Ops:   []string{"append", "range", "trim"},
			},
			{ID: "work_queue", Label: "Work queue"},
		},
		Engines: []Engine{
			{ID: "postgresql", Label: "PostgreSQL", Family: "relational", Weight: "light"},
			{ID: "redis", Label: "Redis", Family: "kv", Weight: "light"},
			{ID: "rabbitmq", Label: "RabbitMQ", Family: "broker", Weight: "light"},
		},
		Pairs: []Pair{
			{
				Job: "main_db", Engine: "postgresql",
				Fit: FitNatural, Build: BuildAvailable,
				Reason: "Primary relational store for lab templates.", Fidelity: FidelityReal,
			},
			{
				Job: "hot_cache", Engine: "redis",
				Fit: FitNatural, Build: BuildAvailable,
				Reason: "In-memory key cache with TTL.", Fidelity: FidelityReal,
			},
			{
				Job: "timeline", Engine: "redis",
				Fit: FitNatural, Build: BuildAvailable,
				Reason: "Sorted sets and streams for ordered events.", Fidelity: FidelityReal,
			},
			{
				Job: "timeline", Engine: "postgresql",
				Fit: FitWorkable, Build: BuildNotBuilt,
				Reason: "Append-only tables work but need a dedicated template.", Fidelity: FidelityReal,
			},
			{
				Job: "work_queue", Engine: "rabbitmq",
				Fit: FitNatural, Build: BuildAvailable,
				Reason: "AMQP queues with ack semantics.", Fidelity: FidelityReal,
			},
			{
				Job: "work_queue", Engine: "redis",
				Fit: FitWorkable, Build: BuildNotBuilt,
				Reason: "Lists and streams can queue work with different failure modes.", Fidelity: FidelityReal,
			},
		},
		Guarantees: []GuaranteesCard{
			{
				Job: "timeline", Engine: "redis",
				Entries: []GuaranteeEntry{
					{
						ID:       "timeline_survives_cache_restart",
						Claim:    "Timelines survive a cache pod restart",
						Chaos:    "pod_kill",
						Status:   "unverified",
						Fidelity: FidelityReal,
					},
				},
			},
		},
	}
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

	for _, p := range c.Pairs {
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
