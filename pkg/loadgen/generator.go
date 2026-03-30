package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// LoadGenerator manages one vegeta attacker per active deployment.
// UNDERSTAND: vegeta sends requests from INSIDE the Go API server process.
// The API server runs inside the K8s cluster (or locally with port-forward),
// so it can reach deployment services via ClusterIP DNS names.
// This is simpler than deploying a separate load-gen pod per deployment.
type LoadGenerator struct {
	mu     sync.Mutex
	active map[string]*attackSession // keyed by namespace
}

type attackSession struct {
	cancel  context.CancelFunc
	profile LoadProfile
	specs   []EndpointSpec // effective specs (registry copy + weight overrides), frozen for this run
	stats   LoadStats
	mu      sync.RWMutex // protects stats reads
}

func NewLoadGenerator() *LoadGenerator {
	return &LoadGenerator{
		active: make(map[string]*attackSession),
	}
}

func loadgenBaseURL(namespace string) string {
	// UNDERSTAND: when running the API locally, set LOADGEN_BASE_URL to the forwarded app-server origin (e.g. http://127.0.0.1:9080).
	if override := os.Getenv("LOADGEN_BASE_URL"); override != "" {
		return override
	}
	return fmt.Sprintf("http://app-server.%s.svc.cluster.local:8080", namespace)
}

// Start begins sending traffic to the app-server in the given namespace.
// If already running, updates the profile (stops and restarts with new config).
// UNDERSTAND: the app-server Service DNS name is "app-server.{namespace}.svc.cluster.local"
// but within the cluster, K8s DNS resolves "app-server.{namespace}" just fine.
func (lg *LoadGenerator) Start(namespace string, profile LoadProfile) error {
	profile = NormalizeProfile(profile)
	if profile.Template == "" {
		return fmt.Errorf("loadgen: template is required")
	}
	specs, ok := cloneEndpointSpecs(profile.Template)
	if !ok {
		return fmt.Errorf("loadgen: unknown template %q", profile.Template)
	}
	if len(profile.Distribution) == 0 {
		return fmt.Errorf("loadgen: no distribution for template %q", profile.Template)
	}
	for i := range specs {
		if w, ok := profile.Distribution[DistributionKey(specs[i])]; ok {
			specs[i].Weight = w
		}
	}

	lg.mu.Lock()
	defer lg.mu.Unlock()

	if existing, ok := lg.active[namespace]; ok {
		existing.cancel()
		delete(lg.active, namespace)
	}

	ctx, cancel := context.WithCancel(context.Background())
	effectiveDist := EffectiveDistribution(specs)
	session := &attackSession{
		cancel:  cancel,
		profile: profile,
		specs:   specs,
		stats: LoadStats{
			Active:       true,
			StartedAt:    ptrTime(time.Now()),
			Distribution: copyDist(effectiveDist),
			StatusCodes:  make(map[string]int),
		},
	}
	lg.active[namespace] = session

	go session.run(ctx, lg, namespace, profile)
	return nil
}

// Stop halts traffic generation for the given namespace.
func (lg *LoadGenerator) Stop(namespace string) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if session, ok := lg.active[namespace]; ok {
		session.cancel()
		session.mu.Lock()
		session.stats.Active = false
		session.mu.Unlock()
		delete(lg.active, namespace)
	}
}

// finish removes the session when the attack ends naturally (finite duration).
func (lg *LoadGenerator) finish(namespace string, s *attackSession) {
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
func (lg *LoadGenerator) GetStats(namespace string) LoadStats {
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

func (s *attackSession) run(ctx context.Context, lg *LoadGenerator, namespace string, profile LoadProfile) {
	baseURL := loadgenBaseURL(namespace)
	specs := s.specs

	pools := fetchParamPools(baseURL, specs)
	for _, spec := range specs {
		if spec.ParamName != "" && spec.PoolEndpoint != "" && len(pools[spec.ParamName]) == 0 {
			slog.Warn("loadgen param pool empty, using random placeholders", "namespace", namespace, "param", spec.ParamName)
		}
	}

	// UNDERSTAND: vegeta.NewAttacker creates an HTTP client that sends requests
	// at the specified rate. We build a custom targeter that distributes requests
	// across endpoints according to the profile weights.
	attacker := vegeta.NewAttacker(
		vegeta.Timeout(5*time.Second),
		vegeta.Workers(10),
		vegeta.MaxWorkers(20),
	)

	targeter := buildTargeter(baseURL, specs, pools)
	rate := vegeta.Rate{Freq: profile.RPS, Per: time.Second}
	attackDur := profile.AttackDuration()

	slog.Info("loadgen started", "namespace", namespace, "template", profile.Template, "rps", profile.RPS, "baseURL", baseURL)

	var vm vegeta.Metrics
	var vmMu sync.Mutex

	// UNDERSTAND: vegeta.Attack returns a channel of vegeta.Result.
	results := attacker.Attack(targeter, rate, attackDur, "cloud9-loadgen")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case res, ok := <-results:
			if !ok {
				vmMu.Lock()
				vm.Close()
				s.pushStats(&vm, profile)
				vmMu.Unlock()
				lg.finish(namespace, s)
				slog.Info("loadgen attack finished", "namespace", namespace)
				return
			}
			vmMu.Lock()
			vm.Add(res)
			vmMu.Unlock()

		case <-ticker.C:
			vmMu.Lock()
			vm.Close()
			s.pushStats(&vm, profile)
			vmMu.Unlock()

		case <-ctx.Done():
			attacker.Stop()
			slog.Info("loadgen stopped", "namespace", namespace)
			return
		}
	}
}

func (s *attackSession) pushStats(vm *vegeta.Metrics, profile LoadProfile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.TotalRequests = vm.Requests
	s.stats.CurrentRPS = float64(profile.RPS)
	// vegeta Success is 0–1; API uses percentage 0–100.
	s.stats.SuccessRate = vm.Success * 100
	s.stats.MeanLatency = vm.Latencies.Mean.Seconds()
	s.stats.P99Latency = vm.Latencies.P99.Seconds()
	s.stats.StatusCodes = copyIntMap(vm.StatusCodes)
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
func fetchParamPools(baseURL string, specs []EndpointSpec) map[string][]string {
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
		values := fetchPool(baseURL, k.endpoint, k.field)
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
func fetchPool(baseURL, endpoint, field string) []string {
	for attempt := 0; attempt < 3; attempt++ {
		values := fetchPoolOnce(baseURL, endpoint, field)
		if len(values) > 0 {
			return values
		}
		if attempt < 2 {
			slog.Info("loadgen pool empty, retrying", "endpoint", endpoint, "attempt", attempt+1)
			time.Sleep(2 * time.Second)
		}
	}
	slog.Warn("loadgen pool empty after retries, using random placeholders", "endpoint", endpoint)
	return nil
}

func fetchPoolOnce(baseURL, endpoint, field string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+endpoint, nil)
	if err != nil {
		return nil
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("failed to fetch pool", "endpoint", endpoint, "error", err)
		return nil
	}
	defer resp.Body.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil
	}
	data, ok := raw[field]
	if !ok {
		return nil
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return nil
	}
	return values
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
			if len(pool) > 0 {
				i := rrIndex[selected.ParamName]
				rrIndex[selected.ParamName] = i + 1
				idx := int(i % uint64(len(pool)))
				path = strings.Replace(path, placeholder, pool[idx], 1)
			} else {
				path = strings.Replace(path, placeholder, generateLoadCode(), 1)
			}
		}
		tgt.URL = baseURL + path

		if selected.BodyTemplate != "" {
			body := strings.ReplaceAll(selected.BodyTemplate, "{{.Rand}}", fmt.Sprintf("%d", rand.Intn(1000000)))
			if selected.ParamName != "" {
				pool := pools[selected.ParamName]
				if len(pool) > 0 {
					body = strings.ReplaceAll(body, "{{.Param}}", pool[rand.Intn(len(pool))])
				}
			}
			tgt.Body = []byte(body)
			tgt.Header = http.Header{"Content-Type": []string{"application/json"}}
		} else {
			tgt.Body = nil
			tgt.Header = nil
		}
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

func ptrTime(t time.Time) *time.Time { return &t }
