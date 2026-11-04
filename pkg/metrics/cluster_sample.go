package metrics

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

const clusterSampleTick = publisherCollectInterval

type clusterSample struct {
	fetchedAt      time.Time
	scalars        map[string]map[string]float64            // cluster key -> namespace -> value
	vectors        map[string]map[string]map[string]float64 // cluster key -> namespace -> label -> value
	buckets        map[string]map[string]map[string]float64 // cluster key -> namespace -> le -> cumulative rate
	podMetricsByNS map[string]*metricsv1beta1.PodMetricsList
}

type collectContextKey struct{}

func withCollectNamespace(ctx context.Context, namespace string) context.Context {
	return context.WithValue(ctx, collectContextKey{}, namespace)
}

func collectNamespaceFrom(ctx context.Context) string {
	ns, _ := ctx.Value(collectContextKey{}).(string)
	return ns
}

var (
	namespaceLabelRE = regexp.MustCompile(`namespace="[^"]*",?`)
)

// MetricsQueryObserver is invoked after a cluster sample refresh (not per session Collect).
type MetricsQueryObserver func(source string, count int)

func (c *MetricsCollector) SetMetricsQueryObserver(fn MetricsQueryObserver) {
	if c == nil {
		return
	}
	c.metricsQueryObserver = fn
}

func (c *MetricsCollector) SetClusterSampleEnabled(enabled bool) {
	if c == nil {
		return
	}
	c.clusterSampleEnabled = enabled
}

// ClusterSampleEnabled reports whether the shared cluster metrics poll should run.
// It is on only when prometheusURL is non-empty and rawEnv is "1" or "true" (any case).
func ClusterSampleEnabled(prometheusURL, rawEnv string) bool {
	if strings.TrimSpace(prometheusURL) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(rawEnv)) {
	case "1", "true":
		return true
	default:
		return false
	}
}

func (c *MetricsCollector) ensureClusterSample(ctx context.Context) *clusterSample {
	if c == nil || !c.clusterSampleEnabled || c.prometheusURL == "" {
		return nil
	}
	c.sampleMu.RLock()
	if c.clusterSample != nil && time.Since(c.clusterSample.fetchedAt) < clusterSampleTick {
		s := c.clusterSample
		c.sampleMu.RUnlock()
		return s
	}
	c.sampleMu.RUnlock()

	v, err, _ := c.sampleFlight.Do("refresh", func() (any, error) {
		c.sampleMu.RLock()
		if c.clusterSample != nil && time.Since(c.clusterSample.fetchedAt) < clusterSampleTick {
			s := c.clusterSample
			c.sampleMu.RUnlock()
			return s, nil
		}
		c.sampleMu.RUnlock()

		sample := c.refreshClusterSample(ctx)
		c.sampleMu.Lock()
		c.clusterSample = sample
		c.sampleMu.Unlock()
		return sample, nil
	})
	if err != nil {
		return nil
	}
	s, _ := v.(*clusterSample)
	return s
}

func (c *MetricsCollector) refreshClusterSample(ctx context.Context) *clusterSample {
	sample := &clusterSample{
		scalars:        make(map[string]map[string]float64),
		vectors:        make(map[string]map[string]map[string]float64),
		buckets:        make(map[string]map[string]map[string]float64),
		podMetricsByNS: make(map[string]*metricsv1beta1.PodMetricsList),
	}
	rr := promRateRange()
	promQueries := 0
	for _, q := range clusterPromScalarQueries(rr) {
		promQueries++
		c.ingestPromVector(ctx, sample, q, false, "")
	}
	for _, spec := range clusterPromBucketQueries(rr) {
		promQueries++
		c.ingestPromVector(ctx, sample, spec.clusterQL, true, spec.key)
	}
	for _, q := range clusterPromVectorQueries(rr) {
		promQueries++
		c.ingestPromVector(ctx, sample, q, false, "pod")
	}
	if c.metricsQueryObserver != nil && promQueries > 0 {
		c.metricsQueryObserver("prometheus", promQueries)
	}
	if c.metricsClient != nil {
		if list, err := c.listAllPodMetrics(ctx); err == nil && list != nil {
			for i := range list.Items {
				pm := &list.Items[i]
				ns := pm.Namespace
				if ns == "" {
					continue
				}
				if sample.podMetricsByNS[ns] == nil {
					sample.podMetricsByNS[ns] = &metricsv1beta1.PodMetricsList{}
				}
				sample.podMetricsByNS[ns].Items = append(sample.podMetricsByNS[ns].Items, *pm)
			}
			if c.metricsQueryObserver != nil {
				c.metricsQueryObserver("metrics_api", 1)
			}
		}
	}
	// Age starts when the sample is usable. Stamping before the HTTP calls
	// makes a slow refresh older than one tick the moment it is stored.
	sample.fetchedAt = time.Now()
	return sample
}

type bucketQuerySpec struct {
	key       string
	clusterQL string
}

func (c *MetricsCollector) ingestPromVector(ctx context.Context, sample *clusterSample, promql string, buckets bool, bucketKey string) {
	resp, err := c.promInstantDirect(ctx, promql)
	if err != nil || resp.Status != "success" {
		return
	}
	if buckets {
		if sample.buckets[bucketKey] == nil {
			sample.buckets[bucketKey] = make(map[string]map[string]float64)
		}
		for i := range resp.Data.Result {
			r := &resp.Data.Result[i]
			ns := r.Metric["namespace"]
			if ns == "" {
				continue
			}
			le := r.Metric["le"]
			val := parsePromValue(r.Value[1])
			if sample.buckets[bucketKey][ns] == nil {
				sample.buckets[bucketKey][ns] = make(map[string]float64)
			}
			sample.buckets[bucketKey][ns][le] = val
		}
		return
	}
	labelKey := ""
	if strings.Contains(promql, " by (namespace, pod)") {
		labelKey = "pod"
	}
	if labelKey != "" {
		if sample.vectors[promql] == nil {
			sample.vectors[promql] = make(map[string]map[string]float64)
		}
		for i := range resp.Data.Result {
			r := &resp.Data.Result[i]
			ns := r.Metric["namespace"]
			lbl := r.Metric[labelKey]
			if ns == "" || lbl == "" {
				continue
			}
			if sample.vectors[promql][ns] == nil {
				sample.vectors[promql][ns] = make(map[string]float64)
			}
			sample.vectors[promql][ns][lbl] = parsePromValue(r.Value[1])
		}
		return
	}
	if sample.scalars[promql] == nil {
		sample.scalars[promql] = make(map[string]float64)
	}
	for i := range resp.Data.Result {
		r := &resp.Data.Result[i]
		ns := r.Metric["namespace"]
		if ns == "" {
			continue
		}
		sample.scalars[promql][ns] = parsePromValue(r.Value[1])
	}
}

func parsePromValue(v any) float64 {
	valStr, ok := v.(string)
	if !ok {
		return 0
	}
	var val float64
	_, _ = fmt.Sscanf(valStr, "%f", &val)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0
	}
	return val
}

func (c *MetricsCollector) promInstantDirect(ctx context.Context, promql string) (prometheusResponse, error) {
	return c.executePromInstant(ctx, promql)
}

func (c *MetricsCollector) lookupSampleScalar(sample *clusterSample, ctx context.Context, promql string) (val float64, present bool, ok bool) {
	if sample == nil {
		return 0, false, false
	}
	ns := collectNamespaceFrom(ctx)
	if ns == "" {
		ns = parseNamespaceFromPromQL(promql)
	}
	if ns == "" {
		return 0, false, false
	}
	if strings.Contains(promql, "histogram_quantile") {
		if v, found := c.sampleHistogramQuantile(sample, promql, ns); found {
			return v, true, true
		}
		return 0, false, true
	}
	clusterQL, okKey := promQLToClusterScalarKey(promql)
	if !okKey {
		return 0, false, false
	}
	byNS := sample.scalars[clusterQL]
	if byNS == nil {
		return 0, false, true
	}
	v, okVal := byNS[ns]
	return v, okVal, true
}

func (c *MetricsCollector) lookupSampleMulti(sample *clusterSample, ctx context.Context, promql, labelKey string) (map[string]float64, bool) {
	if sample == nil || labelKey == "" {
		return nil, false
	}
	ns := collectNamespaceFrom(ctx)
	if ns == "" {
		ns = parseNamespaceFromPromQL(promql)
	}
	if ns == "" {
		return nil, false
	}
	clusterQL, okKey := promQLToClusterVectorKey(promql)
	if !okKey {
		return nil, false
	}
	byNS := sample.vectors[clusterQL]
	if byNS == nil {
		return map[string]float64{}, true
	}
	out := byNS[ns]
	if out == nil {
		return map[string]float64{}, true
	}
	cp := make(map[string]float64, len(out))
	for k, v := range out {
		cp[k] = v
	}
	return cp, true
}

func (c *MetricsCollector) sampleHistogramQuantile(sample *clusterSample, promql, ns string) (float64, bool) {
	phi, inner, ok := parseHistogramQuantile(promql)
	if !ok {
		return 0, false
	}
	bucketKey, ok := bucketKeyFromInner(inner)
	if !ok {
		return 0, false
	}
	byNS := sample.buckets[bucketKey]
	if byNS == nil {
		return 0, false
	}
	buckets := byNS[ns]
	if len(buckets) == 0 {
		return 0, false
	}
	return histogramQuantile(phi, buckets), true
}

func parseHistogramQuantile(promql string) (phi float64, inner string, ok bool) {
	const prefix = "histogram_quantile("
	if !strings.HasPrefix(promql, prefix) {
		return 0, "", false
	}
	rest := promql[len(prefix):]
	comma := strings.Index(rest, ",")
	if comma < 0 {
		return 0, "", false
	}
	phiStr := strings.TrimSpace(rest[:comma])
	inner = strings.TrimSpace(rest[comma+1:])
	if strings.HasSuffix(inner, ")") {
		inner = strings.TrimSuffix(inner, ")")
		inner = strings.TrimSpace(inner)
	}
	p, err := strconv.ParseFloat(phiStr, 64)
	if err != nil {
		return 0, "", false
	}
	return p, inner, true
}

func bucketKeyFromInner(inner string) (string, bool) {
	for _, spec := range clusterPromBucketQueries(promRateRange()) {
		if strings.Contains(inner, spec.metricFragment) {
			return spec.key, true
		}
	}
	return "", false
}

func histogramQuantile(phi float64, buckets map[string]float64) float64 {
	if len(buckets) == 0 {
		return 0
	}
	type pair struct {
		le  float64
		cnt float64
	}
	var pairs []pair
	for leStr, cnt := range buckets {
		le, err := strconv.ParseFloat(leStr, 64)
		if err != nil {
			if leStr == "+Inf" {
				le = math.Inf(1)
			} else {
				continue
			}
		}
		pairs = append(pairs, pair{le: le, cnt: cnt})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].le < pairs[j].le })
	// rate() of a histogram bucket counter stays cumulative across le.
	// The +Inf bucket is the total. Summing every le counts the same requests many times.
	for i := 1; i < len(pairs); i++ {
		if pairs[i].cnt < pairs[i-1].cnt {
			pairs[i].cnt = pairs[i-1].cnt
		}
	}
	total := pairs[len(pairs)-1].cnt
	if total <= 0 {
		return 0
	}
	target := phi * total
	var prevLe float64
	var prevCnt float64
	for _, p := range pairs {
		if p.cnt >= target {
			if p.cnt == prevCnt {
				return p.le
			}
			return prevLe + (p.le-prevLe)*(target-prevCnt)/(p.cnt-prevCnt)
		}
		prevLe = p.le
		prevCnt = p.cnt
	}
	return pairs[len(pairs)-1].le
}

func parseNamespaceFromPromQL(promql string) string {
	m := regexp.MustCompile(`namespace="([^"]+)"`).FindStringSubmatch(promql)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

func (c *MetricsCollector) listAllPodMetrics(ctx context.Context) (*metricsv1beta1.PodMetricsList, error) {
	if c == nil || c.metricsClient == nil {
		return nil, fmt.Errorf("metrics client not configured")
	}
	type listAll interface {
		ListAllPodMetrics(context.Context) (*metricsv1beta1.PodMetricsList, error)
	}
	if all, ok := c.metricsClient.(listAll); ok {
		return all.ListAllPodMetrics(ctx)
	}
	return nil, fmt.Errorf("cluster pod metrics list not supported")
}

func promQLToClusterScalarKey(promql string) (string, bool) {
	if strings.Contains(promql, "histogram_quantile") {
		return "", false
	}
	if parseNamespaceFromPromQL(promql) == "" {
		return "", false
	}
	stripped := strings.TrimSpace(stripNamespaceFromPromQL(promql))
	agg, inner, ok := splitOuterAgg(stripped)
	if !ok {
		return "", false
	}
	// sum(rate(metric{namespace="ns"}[10s])) becomes
	// sum by (namespace) (rate(metric[10s])).
	// Wrapping the whole sum() drops the namespace label inside Prometheus.
	return fmt.Sprintf("%s by (namespace) (%s)", agg, inner), true
}

func splitOuterAgg(q string) (agg, inner string, ok bool) {
	for _, name := range []string{"sum", "max", "min", "avg"} {
		prefix := name + "("
		if strings.HasPrefix(q, prefix) && strings.HasSuffix(q, ")") {
			return name, q[len(prefix) : len(q)-1], true
		}
	}
	return "", "", false
}

func promQLToClusterVectorKey(promql string) (string, bool) {
	if parseNamespaceFromPromQL(promql) == "" {
		return "", false
	}
	inner := strings.TrimSpace(stripNamespaceFromPromQL(promql))
	switch {
	case strings.Contains(inner, "sum by (pod, le)"):
		return strings.Replace(inner, "sum by (pod, le)", "sum by (namespace, pod, le)", 1), true
	case strings.Contains(inner, "sum by (pod)"):
		return strings.Replace(inner, "sum by (pod)", "sum by (namespace, pod)", 1), true
	default:
		return "", false
	}
}

func stripNamespaceFromPromQL(promql string) string {
	out := namespaceLabelRE.ReplaceAllString(promql, "")
	out = strings.ReplaceAll(out, "{,", "{")
	out = strings.ReplaceAll(out, ",}", "}")
	out = strings.ReplaceAll(out, "{}", "")
	return out
}

func (c *MetricsCollector) podMetricsListFromSample(ctx context.Context, namespace string) (*metricsv1beta1.PodMetricsList, bool) {
	sample := c.ensureClusterSample(ctx)
	if sample == nil {
		return nil, false
	}
	list := sample.podMetricsByNS[namespace]
	if list == nil {
		return &metricsv1beta1.PodMetricsList{}, true
	}
	return list, true
}
