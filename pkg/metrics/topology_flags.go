package metrics

// TopologyFlags summarizes the deployed canvas topology for narrative generation.
// Built from the saved design only (not live metrics) so idle components stay visible.
// Callers derive flags from deployer.DeploymentDesign at the API boundary and pass them here
// so this package does not import pkg/deployer.
type TopologyFlags struct {
	HasCache      bool
	HasMQ         bool
	HasWorker     bool
	HasLB         bool
	CacheStrategy string
	Template      string
	// Storage topology (from deployment design, not live metrics).
	HasDB       bool
	DBEngine    string // e.g. "postgresql"
	DBRole      string // e.g. "primary"
	CacheEngine string // e.g. "redis" when HasCache
	CacheRole   string // e.g. "cache-aside" | "primary"
	// HasProxyCache is true when the cache node uses write-through, write-behind, or read-through.
	HasProxyCache bool
}
