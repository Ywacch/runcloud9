package loadgen

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	vegeta "github.com/tsenart/vegeta/v12/lib"

	"github.com/ywacch/cloud9/pkg/templateapp/trace"
	"github.com/ywacch/cloud9/pkg/templates"
)

// capIdentityUniverse keeps the first n ids of the union of pools and filters
// each pool to that set. n <= 0 leaves the pools unchanged. allowed is the
// union after the cap, in cap order.
func capIdentityUniverse(pools [][]string, n int) (capped [][]string, allowed []string) {
	allowed = unionIdentityIDs(pools)
	if n <= 0 {
		return clonePoolSlices(pools), allowed
	}
	if len(allowed) > n {
		allowed = append([]string(nil), allowed[:n]...)
	}
	allowSet := make(map[string]struct{}, len(allowed))
	for _, id := range allowed {
		allowSet[id] = struct{}{}
	}
	capped = make([][]string, len(pools))
	for i, pool := range pools {
		var filtered []string
		for _, id := range pool {
			if _, ok := allowSet[id]; ok {
				filtered = append(filtered, id)
			}
		}
		capped[i] = filtered
	}
	return capped, allowed
}

func clonePoolSlices(pools [][]string) [][]string {
	out := make([][]string, len(pools))
	for i, p := range pools {
		out[i] = append([]string(nil), p...)
	}
	return out
}

func unionIdentityIDs(pools [][]string) []string {
	seen := make(map[string]struct{})
	var ids []string
	for _, pool := range pools {
		for _, id := range pool {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	allNumeric := len(ids) > 0
	numeric := make([]int, 0, len(ids))
	for _, id := range ids {
		n, err := strconv.Atoi(id)
		if err != nil {
			allNumeric = false
			break
		}
		numeric = append(numeric, n)
	}
	if allNumeric {
		sort.Ints(numeric)
		out := make([]string, len(numeric))
		for i, n := range numeric {
			out[i] = strconv.Itoa(n)
		}
		return out
	}
	sort.Strings(ids)
	return ids
}

// withPoolLimit adds limit=n to a pool endpoint's query. n <= 0 returns endpoint unchanged.
func withPoolLimit(endpoint string, n int) string {
	if n <= 0 || endpoint == "" {
		return endpoint
	}
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	q := u.Query()
	q.Set("limit", strconv.Itoa(n))
	u.RawQuery = q.Encode()
	return u.String()
}

// ClientIDCounter counts distinct client ids up to a limit. Safe for concurrent use.
type ClientIDCounter struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	limit int
}

func NewClientIDCounter(limit int) *ClientIDCounter {
	if limit <= 0 {
		limit = 10000
	}
	return &ClientIDCounter{
		seen:  make(map[string]struct{}),
		limit: limit,
	}
}

// Count returns how many distinct ids have been recorded.
func (c *ClientIDCounter) Count() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	n := len(c.seen)
	c.mu.Unlock()
	return n
}

func (c *ClientIDCounter) record(id string) {
	if c == nil || id == "" {
		return
	}
	c.mu.Lock()
	if len(c.seen) >= c.limit {
		if _, ok := c.seen[id]; ok {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		return
	}
	c.seen[id] = struct{}{}
	c.mu.Unlock()
}

// CountClientIDs records the X-C9-Client-ID header of every target inner produces.
func CountClientIDs(inner vegeta.Targeter, counter *ClientIDCounter) vegeta.Targeter {
	if inner == nil || counter == nil {
		return inner
	}
	return func(tgt *vegeta.Target) error {
		if err := inner(tgt); err != nil {
			return err
		}
		if tgt.Header != nil {
			counter.record(tgt.Header.Get(trace.HeaderClientID))
		}
		return nil
	}
}

func identityParamForTemplate(templateID string) string {
	id, ok := templates.GetLoadgenIdentity(templateID)
	if !ok || id == nil {
		return ""
	}
	return id.Param
}

func applyIdentityCap(
	idParam string,
	specs []EndpointSpec,
	paramDefs []ActorParam,
	flat map[string][]string,
	segment map[string][]string,
	n int,
) (int, []string) {
	if idParam == "" {
		return 0, nil
	}
	pools, segKeys := collectIdentityPoolSlices(idParam, specs, paramDefs, flat, segment)
	capped, allowed := capIdentityUniverse(pools, n)
	if len(capped) > 0 {
		flat[idParam] = capped[0]
	}
	for i, key := range segKeys {
		if i+1 < len(capped) {
			segment[key] = capped[i+1]
		}
	}
	return len(allowed), allowed
}

func collectIdentityPoolSlices(
	idParam string,
	specs []EndpointSpec,
	paramDefs []ActorParam,
	flat map[string][]string,
	segment map[string][]string,
) ([][]string, []string) {
	pools := [][]string{append([]string(nil), flat[idParam]...)}
	keys := identitySegmentPoolKeys(idParam, specs, paramDefs)
	for _, key := range keys {
		pools = append(pools, append([]string(nil), segment[key]...))
	}
	return pools, keys
}

func identitySegmentPoolKeys(idParam string, specs []EndpointSpec, paramDefs []ActorParam) []string {
	epKeys := identityEndpointKeys(specs, idParam)
	var out []string
	seen := make(map[string]struct{})
	for _, def := range paramDefs {
		applies := false
		for _, k := range def.ApplyTo {
			if epKeys[k] {
				applies = true
				break
			}
		}
		if !applies {
			continue
		}
		for _, pool := range def.Pools {
			key := segmentPoolKey(pool.PoolEndpoint, pool.PoolField, pool.Name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, key)
		}
	}
	return out
}

func identityEndpointKeys(specs []EndpointSpec, idParam string) map[string]bool {
	m := make(map[string]bool)
	for _, s := range specs {
		if s.ParamName == idParam {
			m[DistributionKey(s)] = true
		}
	}
	return m
}

func filterPoolToAllowed(pool, allowed []string) []string {
	if len(allowed) == 0 {
		return pool
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, id := range allowed {
		allow[id] = struct{}{}
	}
	var out []string
	for _, id := range pool {
		if _, ok := allow[id]; ok {
			out = append(out, id)
		}
	}
	return out
}
