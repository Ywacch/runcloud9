package loadgen

import (
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// ActorModel picks per-request targets with stable virtual-user identity and segment skew.
type ActorModel struct {
	baseURL string

	entries     []catalogEntry
	totalWeight int

	pools        map[string][]string
	segmentPools map[string][]string

	paramDefs   []ActorParam
	paramValues map[string]float64

	segmentApply map[string]segmentApplyRule

	mu        sync.Mutex
	identity  map[string]string
	lastWrite map[string]string
}

type catalogEntry struct {
	spec   EndpointSpec
	weight int
	key    string
}

type segmentApplyRule struct {
	percentKey string
	pools      []ActorPool
}

// TargetPlan is the resolved loadgen attack plan (static expansion or actor-driven).
type TargetPlan struct {
	Actor             *ActorModel
	Static            []ResolvedTarget
	Distribution      map[string]int
	ActiveUsers       int
	IdentityCount     int
	IdentityAllowed   []string
	ActorFlatPools    map[string][]string
	ActorSegmentPools map[string][]string
}

// BuildTargeter returns a vegeta targeter for this plan.
func (p TargetPlan) BuildTargeter() vegeta.Targeter {
	if p.Actor != nil {
		return p.Actor.targeter()
	}
	return buildTargeterFromResolved(p.Static, p.ActiveUsers)
}

func newActorModel(
	baseURL string,
	specs []EndpointSpec,
	distribution map[string]int,
	flatPools map[string][]string,
	segmentPools map[string][]string,
	paramDefs []ActorParam,
	paramValues map[string]float64,
) *ActorModel {
	m := &ActorModel{
		baseURL:      baseURL,
		pools:        flatPools,
		segmentPools: segmentPools,
		paramDefs:    paramDefs,
		paramValues:  paramValues,
		identity:     make(map[string]string),
		lastWrite:    make(map[string]string),
		segmentApply: make(map[string]segmentApplyRule),
	}
	for _, def := range paramDefs {
		if len(def.Pools) == 0 || len(def.ApplyTo) == 0 {
			continue
		}
		rule := segmentApplyRule{percentKey: def.Key, pools: def.Pools}
		for _, endpointKey := range def.ApplyTo {
			m.segmentApply[endpointKey] = rule
		}
	}
	for _, spec := range specs {
		w := spec.Weight
		if w <= 0 {
			continue
		}
		if override, ok := distribution[DistributionKey(spec)]; ok {
			w = override
		}
		if w <= 0 {
			continue
		}
		m.entries = append(m.entries, catalogEntry{
			spec:   spec,
			weight: w,
			key:    DistributionKey(spec),
		})
		m.totalWeight += w
	}
	if m.totalWeight <= 0 && len(m.entries) > 0 {
		m.totalWeight = 1
	}
	return m
}

func (m *ActorModel) targeter() vegeta.Targeter {
	return func(tgt *vegeta.Target) error {
		selected := m.nextResolved()
		tgt.Method = selected.Method
		tgt.URL = selected.URL
		rid := fmt.Sprintf("%x", rand.Uint64())
		h := http.Header{}
		endpointKey := selected.Key
		if endpointKey == "" {
			endpointKey = selected.Method + " " + selected.URL
		}
		ApplyTraceSampleHeaders(h, rid, endpointKey, nil, TraceSampleRate())
		xffSeed := rid
		if selected.ParamValue != "" {
			xffSeed = selected.ParamValue
		}
		ApplyClientIdentityHeaders(h, xffSeed)
		if selected.Body != "" {
			body := strings.ReplaceAll(selected.Body, "{{.Rand}}", strconv.Itoa(rand.Intn(1_000_000)))
			tgt.Body = []byte(body)
			h.Set("Content-Type", "application/json")
		} else {
			tgt.Body = nil
		}
		tgt.Header = h
		return nil
	}
}

func (m *ActorModel) nextResolved() ResolvedTarget {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) == 0 {
		return ResolvedTarget{}
	}
	r := rand.Intn(m.totalWeight)
	cumulative := 0
	var entry catalogEntry
	for _, e := range m.entries {
		cumulative += e.weight
		if r < cumulative {
			entry = e
			break
		}
	}

	spec := entry.spec
	if spec.ParamName == "" {
		return ResolvedTarget{
			URL:    m.baseURL + spec.PathTemplate,
			Method: spec.Method,
			Body:   spec.BodyTemplate,
			Weight: entry.weight,
			Key:    entry.key,
		}
	}

	paramValue := m.resolveParam(spec, entry.key)
	placeholder := "{" + spec.ParamName + "}"
	path := strings.Replace(spec.PathTemplate, placeholder, paramValue, 1)
	body := expandBodyTemplate(spec.BodyTemplate, paramValue)
	if spec.Method == http.MethodPost {
		m.lastWrite[spec.ParamName] = paramValue
	}
	return ResolvedTarget{
		URL:        m.baseURL + path,
		Method:     spec.Method,
		Body:       body,
		Weight:     entry.weight,
		Key:        entry.key,
		ParamValue: paramValue,
	}
}

func (m *ActorModel) resolveParam(spec EndpointSpec, endpointKey string) string {
	if rule, ok := m.segmentApply[endpointKey]; ok {
		if value := m.pickSegmentParam(spec, rule); value != "" {
			m.identity[spec.ParamName] = value
			return value
		}
	}

	if boost := m.paramValues["readAfterPostBoost"]; boost > 0 {
		if spec.Method == http.MethodGet {
			if last, ok := m.lastWrite[spec.ParamName]; ok && last != "" {
				if rand.Float64() < boost/(boost+1) {
					m.identity[spec.ParamName] = last
					return last
				}
			}
		}
	}

	if id, ok := m.identity[spec.ParamName]; ok && id != "" {
		return id
	}

	value := m.pickFlatParam(spec.ParamName)
	m.identity[spec.ParamName] = value
	return value
}

func (m *ActorModel) pickSegmentParam(spec EndpointSpec, rule segmentApplyRule) string {
	if len(rule.pools) == 0 {
		return ""
	}
	percent := m.paramValues[rule.percentKey]
	useSkew := len(rule.pools) >= 2 && rand.Float64()*100 < percent
	pool := rule.pools[len(rule.pools)-1]
	if useSkew {
		pool = rule.pools[0]
	}
	key := segmentPoolKey(pool.PoolEndpoint, pool.PoolField, pool.Name)
	values := m.segmentPools[key]
	if len(values) == 0 {
		return ""
	}
	return values[rand.Intn(len(values))]
}

func (m *ActorModel) pickFlatParam(paramName string) string {
	pool := m.pools[paramName]
	if len(pool) > 0 {
		return pool[rand.Intn(len(pool))]
	}
	return fallbackParamValue(paramName)
}

func segmentPoolKey(endpoint, field, segment string) string {
	return endpoint + "\x00" + field + "\x00" + segment
}

// PodActorConfig serializes actor-mode loadgen settings for the in-namespace pod.
type PodActorConfig struct {
	BaseURL      string              `json:"baseURL"`
	Endpoints    []EndpointSpec      `json:"endpoints"`
	Distribution map[string]int      `json:"distribution"`
	Pools        map[string][]string `json:"pools"`
	SegmentPools map[string][]string `json:"segmentPools"`
	ParamDefs    []ActorParam        `json:"paramDefs"`
	ParamValues  map[string]float64  `json:"paramValues"`
	ActiveUsers  int                 `json:"activeUsers,omitempty"`
}

// BuildTargeterFromPodActor reconstructs an actor targeter in the loadgen pod.
func BuildTargeterFromPodActor(cfg PodActorConfig) vegeta.Targeter {
	model := newActorModel(
		cfg.BaseURL,
		cfg.Endpoints,
		cfg.Distribution,
		cfg.Pools,
		cfg.SegmentPools,
		cfg.ParamDefs,
		cfg.ParamValues,
	)
	return model.targeter()
}
