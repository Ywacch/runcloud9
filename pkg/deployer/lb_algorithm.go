package deployer

// Live LB algorithms emit real nginx upstream methods that session loadgen can demonstrate.
const (
	lbAlgoRoundRobin = "round-robin"
	lbAlgoLeastConn  = "least_conn"
	lbAlgoRandom     = "random"
	lbAlgoIPHash     = "ip_hash"
	lbAlgoLeastTime  = "least_time"
)

var liveLBAlgorithms = map[string]struct{}{
	lbAlgoRoundRobin: {},
	lbAlgoLeastConn:  {},
	lbAlgoRandom:     {},
	lbAlgoIPHash:     {},
	lbAlgoLeastTime:  {},
}

// normalizeLBAlgorithm maps aliases and legacy values onto the live allowlist.
// Unknown values are left unchanged so validateLBAlgorithm can reject them.
func normalizeLBAlgorithm(raw string) string {
	switch raw {
	case "", "round_robin", "round-robin":
		return lbAlgoRoundRobin
	case "least-connections", "least_conn":
		return lbAlgoLeastConn
	case "random":
		return lbAlgoRandom
	case "ip-hash", "ip_hash", "iphash":
		return lbAlgoIPHash
	case "least_time", "least-time", "least-response-time":
		return lbAlgoLeastTime
	default:
		return raw
	}
}

func isLiveLBAlgorithm(algo string) bool {
	_, ok := liveLBAlgorithms[algo]
	return ok
}

// liveLBAlgorithmList returns a stable, human-readable allowlist for error messages.
func liveLBAlgorithmList() string {
	return "round-robin, least_conn, random, ip_hash, or least_time"
}
