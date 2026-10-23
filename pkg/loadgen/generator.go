package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// LoadGenerator drives HTTP load for a deployment namespace.
// LocalLoadGenerator is the in-process Vegeta implementation; remote backends
// can satisfy the same contract later.
type LoadGenerator interface {
	Start(ctx context.Context, namespace string, profile LoadProfile) error
	Stop(ctx context.Context, namespace string) error
	Update(ctx context.Context, namespace string, u LiveUpdate) error
	GetStats(namespace string) LoadStats
}

// LiveUpdate carries optional live RPS and/or endpoint-mix changes for a running attack.
type LiveUpdate struct {
	RPS          *int
	Distribution map[string]int // nil = do not change mix
}

// ErrNotRunning is returned by Update when no attack is active for the namespace.
var ErrNotRunning = errors.New("loadgen: no attack running")

const (
	minLiveRPS = 1
	maxLiveRPS = 5000
)

// LocalLoadGenerator manages one vegeta attacker per active deployment.
// UNDERSTAND: vegeta sends requests from INSIDE the Go API server process.
// The API server runs inside the K8s cluster (or locally with port-forward),
// so it can reach deployment services via ClusterIP DNS names.
// This is simpler than deploying a separate load-gen pod per deployment.
type LocalLoadGenerator struct {
	mu     sync.Mutex
	active map[string]*attackSession // keyed by namespace
}

var _ LoadGenerator = (*LocalLoadGenerator)(nil)

type attackSession struct {
	cancel    context.CancelFunc
	control   *AttackControl
	profile   LoadProfile
	namespace string
	stats     LoadStats
	mu        sync.RWMutex // protects stats reads
	recent    LatencyRing
}

// NewLocalLoadGenerator returns an in-process vegeta load generator.
func NewLocalLoadGenerator() *LocalLoadGenerator {
	return &LocalLoadGenerator{
		active: make(map[string]*attackSession),
	}
}

// NewLoadGenerator is deprecated; use NewLocalLoadGenerator.
func NewLoadGenerator() *LocalLoadGenerator {
	return NewLocalLoadGenerator()
}

// resolveLoadgenBaseURL picks the HTTP origin for vegeta. LOADGEN_BASE_URL overrides everything (local port-forward).
func resolveLoadgenBaseURL(namespace string, profile LoadProfile) string {
	if override := os.Getenv("LOADGEN_BASE_URL"); override != "" {
		return override
	}
	if profile.BaseURL != "" {
		return profile.BaseURL
	}
	return fmt.Sprintf("http://app-server.%s.svc.cluster.local:8080", namespace)
}

// Start begins sending traffic to the app-server in the given namespace.
// If already running, updates the profile (stops and restarts with new config).
// UNDERSTAND: the app-server Service DNS name is "app-server.{namespace}.svc.cluster.local"
// but within the cluster, K8s DNS resolves "app-server.{namespace}" just fine.
func (lg *LocalLoadGenerator) Start(ctx context.Context, namespace string, profile LoadProfile) error {
	profile = NormalizeProfile(profile)
	if profile.VU != nil {
		return fmt.Errorf("loadgen: vu lane requires the loadgen pod")
	}
	plan, err := profileToTargetPlan(ctx, namespace, profile)
	if err != nil {
		return err
	}

	lg.mu.Lock()
	defer lg.mu.Unlock()

	if existing, ok := lg.active[namespace]; ok {
		existing.cancel()
		delete(lg.active, namespace)
	}

	attackCtx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	control := NewAttackControl(profile.RPS)
	control.SetBaseDistribution(profile.Distribution)
	session := &attackSession{
		cancel:    cancel,
		control:   control,
		profile:   profile,
		namespace: namespace,
		stats: LoadStats{
			Active:        true,
			CurrentRPS:    float64(profile.RPS),
			TargetRPS:     float64(profile.RPS),
			ScheduleID:    profile.ScheduleID,
			StartedAt:     ptrTime(now),
			Distribution:  copyDist(plan.Distribution),
			StatusCodes:   make(map[string]int),
			IdentityCount: plan.IdentityCount,
		},
	}
	lg.active[namespace] = session

	go session.run(attackCtx, lg, namespace, profile, control)
	return nil
}

// Update changes the base RPS and/or endpoint mix of a running attack without stopping it.
func (lg *LocalLoadGenerator) Update(ctx context.Context, namespace string, u LiveUpdate) error {
	_ = ctx
	if u.RPS == nil && u.Distribution == nil {
		return fmt.Errorf("loadgen: update requires rps and/or distribution")
	}
	if u.RPS != nil {
		if err := ValidateLiveRPS(*u.RPS); err != nil {
			return err
		}
	}
	lg.mu.Lock()
	session, ok := lg.active[namespace]
	lg.mu.Unlock()
	if !ok || session == nil || session.control == nil {
		return ErrNotRunning
	}
	if u.Distribution != nil {
		if err := ValidateLiveDistribution(session.profile.Template, u.Distribution); err != nil {
			return err
		}
	}
	if u.RPS != nil {
		session.control.SetBaseRPS(*u.RPS)
	}
	if u.Distribution != nil {
		session.control.SetBaseDistribution(u.Distribution)
	}
	return nil
}

// Stop halts traffic generation for the given namespace.
func (lg *LocalLoadGenerator) Stop(ctx context.Context, namespace string) error {
	_ = ctx
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if session, ok := lg.active[namespace]; ok {
		session.cancel()
		session.mu.Lock()
		session.stats.Active = false
		session.mu.Unlock()
		delete(lg.active, namespace)
	}
	return nil
}

// finish removes the session when the attack ends naturally (finite duration).
func (lg *LocalLoadGenerator) finish(namespace string, s *attackSession) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if cur, ok := lg.active[namespace]; ok && cur == s {
		s.mu.Lock()
		s.stats.Active = false
		s.mu.Unlock()
		delete(lg.active, namespace)
	}
}

// GetStats returns the current load stats for a namespace (or a zero-value inactive stats).
func (lg *LocalLoadGenerator) GetStats(namespace string) LoadStats {
	lg.mu.Lock()
	session, ok := lg.active[namespace]
	lg.mu.Unlock()

	if !ok {
		return LoadStats{Active: false, StatusCodes: map[string]int{}, Distribution: map[string]int{}}
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.stats
}

func (s *attackSession) run(ctx context.Context, lg *LocalLoadGenerator, namespace string, profile LoadProfile, control *AttackControl) {
	slog.Info("loadgen started", "namespace", namespace, "template", profile.Template, "rps", profile.RPS, "schedule", profile.ScheduleID)

	RunAttackLoop(AttackLoopOpts{
		Ctx:           ctx,
		Profile:       profile,
		Namespace:     namespace,
		BuildTargeter: LocalTargeterBuilder(namespace),
		Stats:         &s.stats,
		StatsMu:       &s.mu,
		Recent:        &s.recent,
		Control:       control,
		AttackName:    "cloud9-loadgen",
		OnNaturalFinish: func() {
			lg.finish(namespace, s)
			slog.Info("loadgen attack finished", "namespace", namespace)
		},
	})

	if ctx.Err() != nil {
		slog.Info("loadgen stopped", "namespace", namespace)
	}
}

func copyIntMap(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyDist(m map[string]int) map[string]int {
	return copyIntMap(m)
}

type poolPair struct {
	endpoint string
	field    string
}

// fetchParamPools fetches parameter pools for all endpoints that need them.
// Returns map[paramName][]string. Deduplicates HTTP GET by (PoolEndpoint, PoolField).
func fetchParamPools(ctx context.Context, baseURL string, specs []EndpointSpec, idParam string, poolLimit int) map[string][]string {
	pools := make(map[string][]string)
	byKey := make(map[poolPair][]string)
	for _, s := range specs {
		if s.PoolEndpoint == "" || s.PoolField == "" || s.ParamName == "" {
			continue
		}
		k := poolPair{endpoint: s.PoolEndpoint, field: s.PoolField}
		byKey[k] = append(byKey[k], s.ParamName)
	}
	for k, paramNames := range byKey {
		endpoint := k.endpoint
		if poolLimit > 0 && idParam != "" {
			for _, pn := range paramNames {
				if pn == idParam {
					endpoint = withPoolLimit(endpoint, poolLimit)
					break
				}
			}
		}
		values := fetchPool(ctx, baseURL, endpoint, k.field)
		if len(values) == 0 {
			continue
		}
		for _, pn := range paramNames {
			if _, set := pools[pn]; !set {
				pools[pn] = values
			}
		}
	}
	return pools
}

// fetchPool retries a few times — covers the window after seeding where the app may not yet return rows.
func fetchPool(ctx context.Context, baseURL, endpoint, field string) []string {
	for attempt := 0; attempt < 3; attempt++ {
		values := fetchPoolOnce(ctx, baseURL, endpoint, field)
		if len(values) > 0 {
			return values
		}
		if attempt < 2 {
			slog.Info("loadgen pool empty, retrying", "endpoint", endpoint, "attempt", attempt+1)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
	}
	slog.Warn("loadgen pool empty after retries, using random placeholders", "endpoint", endpoint)
	return nil
}

func fetchPoolOnce(ctx context.Context, baseURL, endpoint, field string) []string {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, baseURL+endpoint, nil)
	if err != nil {
		return nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("failed to fetch pool", "endpoint", endpoint, "error", err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		slog.Warn("failed to fetch pool: non-2xx response", "endpoint", endpoint, "status", resp.StatusCode)
		return nil
	}

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}
	data, ok := raw[field]
	if !ok {
		slog.Warn("pool field missing in response", "endpoint", endpoint, "field", field)
		return nil
	}
	values, ok := parsePoolValues(data)
	if !ok {
		slog.Warn("pool field has unsupported type", "endpoint", endpoint, "field", field)
		return nil
	}
	return values
}

func parsePoolValues(data json.RawMessage) ([]string, bool) {
	var stringsOnly []string
	if err := json.Unmarshal(data, &stringsOnly); err == nil {
		return stringsOnly, true
	}

	var mixed []any
	if err := json.Unmarshal(data, &mixed); err != nil {
		return nil, false
	}
	out := make([]string, 0, len(mixed))
	for _, v := range mixed {
		switch t := v.(type) {
		case string:
			out = append(out, t)
		case float64:
			if math.Trunc(t) == t {
				out = append(out, strconv.FormatInt(int64(t), 10))
			} else {
				out = append(out, strconv.FormatFloat(t, 'f', -1, 64))
			}
		default:
			return nil, false
		}
	}
	return out, true
}

// buildTargeter creates a vegeta.Targeter from generic EndpointSpecs.
// UNDERSTAND: vegeta calls the targeter from multiple goroutines — one mutex covers rand and pool indexing.
func buildTargeter(baseURL string, specs []EndpointSpec, pools map[string][]string) vegeta.Targeter {
	type weighted struct {
		spec   EndpointSpec
		weight int
	}
	var targets []weighted
	var totalWeight int
	for _, s := range specs {
		w := s.Weight
		if w <= 0 {
			continue
		}
		targets = append(targets, weighted{spec: s, weight: w})
		totalWeight += w
	}
	if len(targets) == 0 && len(specs) > 0 {
		targets = append(targets, weighted{spec: specs[0], weight: 1})
		totalWeight = 1
	}
	if totalWeight == 0 {
		totalWeight = 1
	}

	var mu sync.Mutex
	rrIndex := make(map[string]uint64)

	return func(tgt *vegeta.Target) error {
		mu.Lock()
		defer mu.Unlock()

		r := rand.Intn(totalWeight)
		cumulative := 0
		var selected EndpointSpec
		for _, t := range targets {
			cumulative += t.weight
			if r < cumulative {
				selected = t.spec
				break
			}
		}

		tgt.Method = selected.Method
		path := selected.PathTemplate
		if selected.ParamName != "" {
			placeholder := "{" + selected.ParamName + "}"
			pool := pools[selected.ParamName]
			paramValue := fallbackParamValue(selected.ParamName)
			if len(pool) > 0 {
				i := rrIndex[selected.ParamName]
				rrIndex[selected.ParamName] = i + 1
				idx := int(i % uint64(len(pool)))
				paramValue = pool[idx]
			}
			path = strings.Replace(path, placeholder, paramValue, 1)
		}
		tgt.URL = baseURL + path

		rid := fmt.Sprintf("%x", rand.Uint64())
		h := http.Header{}
		ApplyTraceSampleHeaders(h, rid, DistributionKey(selected), nil, TraceSampleRate())
		ApplyClientIdentityHeaders(h, rid)
		if selected.BodyTemplate != "" {
			body := strings.ReplaceAll(selected.BodyTemplate, "{{.Rand}}", fmt.Sprintf("%d", rand.Intn(1000000)))
			if selected.ParamName != "" {
				pool := pools[selected.ParamName]
				paramValue := fallbackParamValue(selected.ParamName)
				if len(pool) > 0 {
					paramValue = pool[rand.Intn(len(pool))]
				}
				body = strings.ReplaceAll(body, "{{.Param}}", paramValue)
			}
			tgt.Body = []byte(body)
			h.Set("Content-Type", "application/json")
		} else {
			tgt.Body = nil
		}
		tgt.Header = h
		return nil
	}
}

func generateLoadCode() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	code := make([]byte, 6)
	for i := range code {
		code[i] = charset[rand.Intn(len(charset))]
	}
	return string(code)
}

func fallbackParamValue(paramName string) string {
	switch strings.ToLower(paramName) {
	case "productid":
		// Keep ecommerce placeholders syntactically valid for path+JSON bodies.
		return strconv.Itoa(1 + rand.Intn(1000))
	default:
		return generateLoadCode()
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// ValidateLiveRPS rejects out-of-range live RPS updates.
func ValidateLiveRPS(rps int) error {
	if rps < minLiveRPS {
		return fmt.Errorf("loadgen: rps must be at least %d", minLiveRPS)
	}
	if rps > maxLiveRPS {
		return fmt.Errorf("loadgen: rps must be at most %d", maxLiveRPS)
	}
	return nil
}

// ValidateLiveRPSForTier applies ValidateLiveRPS then a subscription tier ceiling.
// tierMax <= 0 skips the tier check (store unavailable in tests).
func ValidateLiveRPSForTier(rps int, tierMax int) error {
	if err := ValidateLiveRPS(rps); err != nil {
		return err
	}
	if tierMax > 0 && rps > tierMax {
		return fmt.Errorf("loadgen: rps must be at most %d for your subscription tier", tierMax)
	}
	return nil
}

// ValidateLiveDistribution rejects unknown keys, negative weights, or all-zero mixes.
// Every endpoint declared for templateID must appear in dist.
func ValidateLiveDistribution(templateID string, dist map[string]int) error {
	if templateID == "" {
		return fmt.Errorf("loadgen: template is required for distribution validation")
	}
	specs, ok := cloneEndpointSpecs(templateID)
	if !ok {
		return fmt.Errorf("loadgen: unknown template %q", templateID)
	}
	if len(dist) == 0 {
		return fmt.Errorf("loadgen: distribution must not be empty")
	}
	expected := make(map[string]struct{}, len(specs))
	for _, s := range specs {
		expected[DistributionKey(s)] = struct{}{}
	}
	for key := range expected {
		if _, present := dist[key]; !present {
			return fmt.Errorf("loadgen: distribution missing endpoint %q", key)
		}
	}
	sum := 0
	for key, weight := range dist {
		if _, present := expected[key]; !present {
			return fmt.Errorf("loadgen: unknown distribution key %q", key)
		}
		if weight < 0 {
			return fmt.Errorf("loadgen: distribution weight for %q must be >= 0", key)
		}
		sum += weight
	}
	if sum <= 0 {
		return fmt.Errorf("loadgen: distribution weights must sum to greater than 0")
	}
	return nil
}
