package deployer

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourceLimitsJSON is the stable API/catalog representation of CPU and memory limits.
type ResourceLimitsJSON struct {
	CPU              string `json:"cpu"`
	Memory           string `json:"memory"`
	EphemeralStorage string `json:"ephemeralStorage"`
	Profile          string `json:"profile,omitempty"`
}

// ResourceCatalogResponse is returned by GET /meta/resource-profiles.
type ResourceCatalogResponse struct {
	Profiles   map[string]ResourceLimitsJSON `json:"profiles"`
	Components map[string]ResourceLimitsJSON `json:"components"`
}

// NewResourceCatalogResponse builds the public catalog payload for the API and frontend.
func NewResourceCatalogResponse() ResourceCatalogResponse {
	return ResourceCatalogResponse{
		Profiles: map[string]ResourceLimitsJSON{
			"small":  limitsJSON("125m", "256Mi", "100Mi", "small"),
			"medium": limitsJSON("250m", "512Mi", "100Mi", ""),
			"large":  limitsJSON("500m", "1Gi", "200Mi", ""),
			"alpha":  limitsJSON("125m", "256Mi", "100Mi", "small"),
		},
		Components: map[string]ResourceLimitsJSON{
			"db":       limitsJSON("125m", "256Mi", "100Mi", "small"),
			"mq":       limitsJSON("200m", "256Mi", "100Mi", ""),
			"lb":       limitsJSON("250m", "128Mi", "50Mi", ""),
			"exporter": limitsJSON("25m", "32Mi", "50Mi", ""),
		},
	}
}

func limitsJSON(cpu, mem, ephemeral, profile string) ResourceLimitsJSON {
	out := ResourceLimitsJSON{
		CPU:              cpu,
		Memory:           mem,
		EphemeralStorage: ephemeral,
	}
	if profile != "" {
		out.Profile = profile
	}
	return out
}

// ResourceProfile returns Kubernetes resources for a named sizing profile.
func ResourceProfile(profile string) corev1.ResourceRequirements {
	switch profile {
	case "large":
		return resourceRequirements("500m", "1Gi", "200Mi")
	case "alpha", "small":
		return resourceRequirements("125m", "256Mi", "100Mi")
	default: // medium and unknown
		return DefaultResources()
	}
}

// DefaultResources returns the standard 250m/512Mi resource spec (medium profile).
func DefaultResources() corev1.ResourceRequirements {
	return resourceRequirements("250m", "512Mi", "100Mi")
}

// DBResources returns default Postgres workload sizing when no instanceSize is set.
func DBResources() corev1.ResourceRequirements {
	return ComponentDefaultResources("db")
}

// ExporterResources returns minimal resources for metrics sidecars.
func ExporterResources() corev1.ResourceRequirements {
	return componentRequirements("exporter")
}

// ComponentDefaultResources returns provisioned limits for a node type when resourceProfile is unset.
func ComponentDefaultResources(nodeType string) corev1.ResourceRequirements {
	return componentRequirements(nodeType)
}

func componentRequirements(component string) corev1.ResourceRequirements {
	switch component {
	case "db":
		return resourceRequirements("125m", "256Mi", "100Mi")
	case "mq":
		return resourceRequirements("200m", "256Mi", "100Mi")
	case "lb":
		return resourceRequirements("250m", "128Mi", "50Mi")
	case "exporter":
		return resourceRequirements("25m", "32Mi", "50Mi")
	default:
		return ResourceProfile("small")
	}
}

// ResolveNodeResources picks profile overrides from config or component defaults.
func ResolveNodeResources(nodeType string, config map[string]interface{}) corev1.ResourceRequirements {
	if prof := configString(config, "resourceProfile", ""); prof != "" {
		return ResourceProfile(prof)
	}
	return ComponentDefaultResources(nodeType)
}

// cacheProfileName returns the canvas sizing profile for a cache node's Redis.
// Empty config matches ResolveNodeResources(cache) → small.
func cacheProfileName(config map[string]interface{}) string {
	if prof := configString(config, "resourceProfile", ""); prof != "" {
		return prof
	}
	return "small"
}

// CacheProxyResources returns limits for the caching-proxy Deployment.
// When proxyResourceProfile is set, that profile selects the proxy size table
// (independent of Redis). When unset, Redis resourceProfile drives the same
// table so defaults match C3 (proxy amounts embed one tier above Redis):
//
//	Redis small  → proxy 250m / 512Mi
//	Redis medium → proxy 500m / 1Gi
//	Redis large  → proxy 750m / 1536Mi
//
// Explicit proxy profiles use the same amount table keyed by proxy size name.
func CacheProxyResources(config map[string]interface{}) corev1.ResourceRequirements {
	if proxyProf := configString(config, "proxyResourceProfile", ""); proxyProf != "" {
		return proxyResourcesForProfile(proxyProf)
	}
	return proxyResourcesForProfile(defaultProxyProfileForRedis(cacheProfileName(config)))
}

// DefaultProxyResourceProfile returns the proxy size name used when the canvas
// does not set proxyResourceProfile (aligned with CacheProxyResources defaults).
func DefaultProxyResourceProfile(redisProfile string) string {
	return defaultProxyProfileForRedis(redisProfile)
}

func defaultProxyProfileForRedis(redisProfile string) string {
	switch redisProfile {
	case "large":
		return "large"
	case "medium":
		return "medium"
	default: // small, alpha, unknown
		return "small"
	}
}

// proxyResourcesForProfile maps a proxy size name to cache-proxy limits.
// Amounts differ from ResourceProfile (Redis) — proxy is sized for HTTP+DB work.
func proxyResourcesForProfile(profile string) corev1.ResourceRequirements {
	switch profile {
	case "large":
		return resourceRequirements("750m", "1536Mi", "200Mi")
	case "medium":
		return resourceRequirements("500m", "1Gi", "200Mi")
	default: // small, alpha, unknown
		return resourceRequirements("250m", "512Mi", "100Mi")
	}
}

func resourceRequirements(cpu, mem, ephemeral string) corev1.ResourceRequirements {
	cpuQty := resource.MustParse(cpu)
	memQty := resource.MustParse(mem)
	ephemeralQty := resource.MustParse(ephemeral)
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              cpuQty,
			corev1.ResourceMemory:           memQty,
			corev1.ResourceEphemeralStorage: ephemeralQty,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              cpuQty.DeepCopy(),
			corev1.ResourceMemory:           memQty.DeepCopy(),
			corev1.ResourceEphemeralStorage: ephemeralQty.DeepCopy(),
		},
	}
}
