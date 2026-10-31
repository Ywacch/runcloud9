package engines

// Fidelity labels how close a job–engine pair or guarantee claim is to production behavior.
// Values match pkg/rules so callers can compare without importing rules from this package.
const (
	FidelityReal       = "real"
	FidelitySimulated  = "simulated"
	FidelityCompressed = "compressed"
)

// CatalogVersion bumps when the served JSON shape or semantics change.
const CatalogVersion = "2"

// Fit describes how naturally an engine serves a job.
type Fit string

const (
	FitNatural    Fit = "natural"
	FitWorkable   Fit = "workable"
	FitPoor       Fit = "poor"
	FitImpossible Fit = "impossible"
)

// Build describes whether we ship a template image for the pair today.
type Build string

const (
	BuildAvailable Build = "available"
	BuildNotBuilt  Build = "not_built"
	BuildPlanned   Build = "planned"
)

// Shape is the minimum pod footprint for one engine instance.
type Shape struct {
	Pods     int   `json:"pods" yaml:"pods"`
	CPUMilli int64 `json:"cpuMilli" yaml:"cpuMilli"`
	MemoryMi int64 `json:"memoryMi" yaml:"memoryMi"`
}

// Job is a platform workload role (main DB, cache, timeline, queue).
type Job struct {
	ID        string   `json:"id" yaml:"id"`
	Label     string   `json:"label" yaml:"label"`
	Ops       []string `json:"ops,omitempty" yaml:"ops,omitempty"`
	OpMetrics []string `json:"opMetrics,omitempty" yaml:"opMetrics,omitempty"`
}

// Engine is a datastore or broker family we can mount in a lab.
type Engine struct {
	ID            string   `json:"id" yaml:"id"`
	Label         string   `json:"label" yaml:"label"`
	Family        string   `json:"family" yaml:"family"`
	WireCompat    string   `json:"wireCompat,omitempty" yaml:"wireCompat,omitempty"`
	Weight        string   `json:"weight" yaml:"weight"`
	MinShape      Shape    `json:"minShape" yaml:"minShape"`
	NativeSignals []string `json:"nativeSignals,omitempty" yaml:"nativeSignals,omitempty"`
	Exporter      string   `json:"exporter,omitempty" yaml:"exporter,omitempty"`
}

// Demo links a poor-fit pair to a chaos scenario and check rule for teaching.
type Demo struct {
	Chaos string `json:"chaos" yaml:"chaos"`
	Check string `json:"check" yaml:"check"`
}

// Pair is one job matched to one engine with fit, build state, and fidelity.
type Pair struct {
	Job      string `json:"job" yaml:"job"`
	Engine   string `json:"engine" yaml:"engine"`
	Fit      Fit    `json:"fit" yaml:"fit"`
	Build    Build  `json:"build" yaml:"build"`
	Reason   string `json:"reason" yaml:"reason"`
	Fidelity string `json:"fidelity" yaml:"fidelity"`
	Demo     *Demo  `json:"failureDemo,omitempty" yaml:"failureDemo,omitempty"`
}

// GuaranteeEntry is one claim on a guarantees card.
type GuaranteeEntry struct {
	ID       string `json:"id" yaml:"id"`
	Claim    string `json:"claim" yaml:"claim"`
	Chaos    string `json:"chaos,omitempty" yaml:"chaos,omitempty"`
	Check    string `json:"check,omitempty" yaml:"check,omitempty"`
	Status   string `json:"status" yaml:"status"`
	Fidelity string `json:"fidelity" yaml:"fidelity"`
	PACELC   string `json:"pacelc,omitempty" yaml:"pacelc,omitempty"`
}

// GuaranteesCard groups claims for one job–engine pair.
type GuaranteesCard struct {
	Job     string           `json:"job" yaml:"job"`
	Engine  string           `json:"engine" yaml:"engine"`
	Entries []GuaranteeEntry `json:"entries" yaml:"entries"`
}

// Catalog is the full jobs registry served at GET /meta/jobs.
type Catalog struct {
	Version    string           `json:"catalogVersion"`
	Jobs       []Job            `json:"jobs"`
	Engines    []Engine         `json:"engines"`
	Pairs      []Pair           `json:"pairs"`
	Guarantees []GuaranteesCard `json:"guarantees"`
}
