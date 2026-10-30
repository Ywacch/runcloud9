package engines

// Fidelity labels how close a job–engine pair or guarantee claim is to production behavior.
// Values match pkg/rules so callers can compare without importing rules from this package.
const (
	FidelityReal       = "real"
	FidelitySimulated  = "simulated"
	FidelityCompressed = "compressed"
)

// CatalogVersion bumps when the served JSON shape or semantics change.
const CatalogVersion = "1"

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

// Job is a platform workload role (main DB, cache, timeline, queue).
type Job struct {
	ID        string   `json:"id"`
	Label     string   `json:"label"`
	Ops       []string `json:"ops,omitempty"`
	OpMetrics []string `json:"opMetrics,omitempty"`
}

// Engine is a datastore or broker family we can mount in a lab.
type Engine struct {
	ID         string `json:"id"`
	Label      string `json:"label"`
	Family     string `json:"family"`
	WireCompat string `json:"wireCompat,omitempty"`
	Weight     string `json:"weight"`
}

// Demo links a poor-fit pair to a chaos scenario and check rule for teaching.
type Demo struct {
	Chaos string `json:"chaos"`
	Check string `json:"check"`
}

// Pair is one job matched to one engine with fit, build state, and fidelity.
type Pair struct {
	Job      string `json:"job"`
	Engine   string `json:"engine"`
	Fit      Fit    `json:"fit"`
	Build    Build  `json:"build"`
	Reason   string `json:"reason"`
	Fidelity string `json:"fidelity"`
	Demo     *Demo  `json:"failureDemo,omitempty"`
}

// GuaranteeEntry is one claim on a guarantees card.
type GuaranteeEntry struct {
	ID       string `json:"id"`
	Claim    string `json:"claim"`
	Chaos    string `json:"chaos,omitempty"`
	Check    string `json:"check,omitempty"`
	Status   string `json:"status"`
	Fidelity string `json:"fidelity"`
	PACELC   string `json:"pacelc,omitempty"`
}

// GuaranteesCard groups claims for one job–engine pair.
type GuaranteesCard struct {
	Job     string           `json:"job"`
	Engine  string           `json:"engine"`
	Entries []GuaranteeEntry `json:"entries"`
}

// Catalog is the full jobs registry served at GET /meta/jobs.
type Catalog struct {
	Version    string           `json:"catalogVersion"`
	Jobs       []Job            `json:"jobs"`
	Engines    []Engine         `json:"engines"`
	Pairs      []Pair           `json:"pairs"`
	Guarantees []GuaranteesCard `json:"guarantees"`
}
