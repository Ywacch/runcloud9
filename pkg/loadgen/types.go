package loadgen

import (
	"fmt"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/templates"
)

// maxActiveUsers caps LoadProfile.ActiveUsers so a client cannot request an unbounded identity set.
const maxActiveUsers = 10000

// EndpointSpec describes one endpoint the load generator can target.
// UNDERSTAND: templates define their own endpoints — the load generator
// doesn't need to know about /shorten or /r/{code} specifically.
type EndpointSpec struct {
	Method       string `json:"method"`       // "GET" or "POST"
	PathTemplate string `json:"pathTemplate"` // "/r/{code}", "/feed/{userId}", "/products"
	ParamName    string `json:"paramName"`    // "code", "userId", "" — name of the {param} in path
	PoolEndpoint string `json:"poolEndpoint"` // "/code-pool", "/user-pool", "" — endpoint to fetch param values
	PoolField    string `json:"poolField"`    // "codes", "userIds" — JSON field name in pool response
	BodyTemplate string `json:"bodyTemplate"` // `{"url":"https://example.com/page/{{.Rand}}"}` or "" for no body
	Weight       int    `json:"weight"`       // distribution weight (e.g. 80, 20)
}

// DistributionKey is the stable key for optional distribution overrides and LoadStats.
// Format: Method + " " + PathTemplate (e.g. "GET /r/{code}").
func DistributionKey(s EndpointSpec) string {
	return s.Method + " " + s.PathTemplate
}

// ResolvedTarget is a fully expanded HTTP target for vegeta (static mode) or one actor tick.
type ResolvedTarget struct {
	URL        string `json:"url"`
	Method     string `json:"method"`
	Body       string `json:"body"`
	Weight     int    `json:"weight"`
	Key        string `json:"key,omitempty"`
	ParamValue string `json:"paramValue,omitempty"`
}

// LoadProfile defines the traffic shape for a deployment.
// UNDERSTAND: the frontend sends this when starting/updating load.
// All fields have defaults so the user can start with just "start"
// and tweak later.
type LoadProfile struct {
	RPS             int    `json:"rps"`             // requests per second (default: 50)
	DurationSeconds int64  `json:"durationSeconds"` // 0 = run until stopped (default: 0)
	Template        string `json:"template"`        // "url-shortener" — looks up EndpointSpecs in registry
	// BaseURL optional origin (e.g. http://lb.ns.svc.cluster.local:80). Env LOADGEN_BASE_URL wins if set.
	BaseURL string `json:"baseUrl,omitempty"`
	// Distribution optional override: keys are DistributionKey (e.g. "GET /r/{code}") → weight.
	Distribution map[string]int `json:"distribution"`
	// ActorParams overrides template-declared actor param defaults (validated against YAML schema).
	ActorParams map[string]float64 `json:"actorParams,omitempty"`
	// ScheduleID selects a template-declared load schedule (YAML loadgen.schedules).
	ScheduleID string `json:"scheduleId,omitempty"`
	// ActiveUsers is the number of distinct logical identities the pressure lane draws from.
	// Zero keeps today's behavior: actor mode uses the template pool, static mode a new seed per request.
	ActiveUsers int `json:"activeUsers,omitempty"`
	// Phases is resolved from the template schedule during normalization (not sent by clients).
	Phases []Phase `json:"-"`
	// VU (virtual user) selects closed-loop scripted traffic (post, then reply)
	// beside Vegeta's open-loop requests per second. Loadgen pod only.
	VU *VUConfig `json:"vu,omitempty"`
}

// AttackDuration returns vegeta attack duration; 0 means run until Stop().
func (p LoadProfile) AttackDuration() time.Duration {
	if p.DurationSeconds <= 0 {
		return 0
	}
	return time.Duration(p.DurationSeconds) * time.Second
}

// LoadStats is streamed via the metrics WebSocket alongside DeploymentMetrics.
// UNDERSTAND: we embed load stats into the existing metrics stream rather than
// creating a separate WebSocket — one connection per deployment keeps the frontend simple.
type LoadStats struct {
	Active            bool                `json:"active"`                    // is load running?
	CurrentRPS        float64             `json:"currentRps"`                // configured RPS (target); legacy alias for TargetRPS
	TargetRPS         float64             `json:"targetRps"`                 // configured RPS (target)
	ActualRPS         float64             `json:"actualRps"`                 // achieved RPS from Vegeta throughput (cumulative)
	TotalRequests     uint64              `json:"totalRequests"`             // cumulative since start
	SuccessRate       float64             `json:"successRate"`               // percentage 0-100
	MeanLatency       float64             `json:"meanLatency"`               // seconds (cumulative run)
	P99Latency        float64             `json:"p99Latency"`                // seconds (cumulative run)
	IntervalP99Ms     float64             `json:"intervalP99Ms"`             // p99 ms in last closed 5s window (client-side)
	IntervalActualRPS float64             `json:"intervalActualRps"`         // RPS in last closed 5s window
	RecentIntervals   []LoadIntervalStats `json:"recentIntervals,omitempty"` // last N closed 5s buckets
	// RecentLatenciesMs is a snapshot of the last RecentLatenciesCap request latencies (ms, client-observed).
	RecentLatenciesMs []float64      `json:"recentLatenciesMs,omitempty"`
	DurationSeconds   float64        `json:"durationSeconds"` // elapsed run time in seconds
	StatusCodes       map[string]int `json:"statusCodes"`     // "200" → count, "500" → count
	Distribution      map[string]int `json:"distribution"`    // effective weights per DistributionKey
	StartedAt         *time.Time     `json:"startedAt"`       // nil if not active
	ScheduleID        string         `json:"scheduleId,omitempty"`
	CurrentPhase      string         `json:"currentPhase,omitempty"`
	PhaseIndex        int            `json:"phaseIndex,omitempty"`
	// Stale is true when the last-known stats are returned after a fetch error.
	Stale bool `json:"stale,omitempty"`
	// VU (virtual user) reports closed-loop script stats when that traffic is active.
	VU *vu.Stats `json:"vu,omitempty"`
	// DetectionMayBeIncomplete is true when Vegeta throughput lags target enough that observer counts may undercount.
	DetectionMayBeIncomplete bool `json:"detectionMayBeIncomplete,omitempty"`
}

// DefaultProfile returns default RPS only. Template and distribution come from the handler or JSON.
func DefaultProfile() LoadProfile {
	return LoadProfile{RPS: 50}
}

// NormalizeProfile applies defaults and merges distribution from template loadgen endpoints.
// Validation errors are ignored; prefer ValidateAndNormalizeProfile at API boundaries.
func NormalizeProfile(p LoadProfile) LoadProfile {
	out, _ := ValidateAndNormalizeProfile(p)
	return out
}

// ValidateAndNormalizeProfile applies defaults, validates actor params, and merges distribution.
func ValidateAndNormalizeProfile(p LoadProfile) (LoadProfile, error) {
	def := DefaultProfile()
	if p.RPS <= 0 {
		if p.Template != "" {
			if t := templates.GetTemplate(p.Template); t != nil && t.LoadgenDefaultRPS > 0 {
				p.RPS = t.LoadgenDefaultRPS
			} else {
				p.RPS = def.RPS
			}
		} else {
			p.RPS = def.RPS
		}
	}
	if p.DurationSeconds < 0 {
		p.DurationSeconds = 0
	}
	if p.ActiveUsers < 0 {
		return p, fmt.Errorf("loadgen: activeUsers must be >= 0")
	}
	if p.ActiveUsers > maxActiveUsers {
		p.ActiveUsers = maxActiveUsers
	}

	if p.VU != nil && p.Template == "" {
		return p, fmt.Errorf("loadgen: vu lane requires a template")
	}

	if p.Template == "" {
		return p, nil
	}

	values, err := mergeActorParamValues(p.Template, p.ActorParams)
	if err != nil {
		return p, err
	}
	p.ActorParams = values

	phases, err := resolveSchedulePhases(p)
	if err != nil {
		return p, err
	}
	if err := validatePhaseActorOverrides(p.Template, phases); err != nil {
		return p, err
	}
	p.Phases = phases

	specs, ok := cloneEndpointSpecs(p.Template)
	if !ok {
		return p, nil
	}

	base := make(map[string]int, len(specs))
	for _, s := range specs {
		base[DistributionKey(s)] = s.Weight
	}

	if len(p.Distribution) > 0 {
		for k, v := range p.Distribution {
			base[k] = v
		}
	}
	p.Distribution = base
	if err := validateVUProfile(p); err != nil {
		return p, err
	}
	return p, nil
}

// EffectiveDistribution returns the weight map for a resolved spec slice (after overrides).
func EffectiveDistribution(specs []EndpointSpec) map[string]int {
	m := make(map[string]int, len(specs))
	for _, s := range specs {
		m[DistributionKey(s)] = s.Weight
	}
	return m
}
