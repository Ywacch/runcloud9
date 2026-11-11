package main

import (
	"strconv"
	"strings"
)

const (
	// Defaults cover a 100-session orchestration burst. Each clientset gets its
	// own bucket at these values (see kubeobs.IsolatedConfig).
	defaultKubeClientQPS   float32 = 400
	defaultKubeClientBurst int     = 800
)

// kubeClientLimits reads K8S_CLIENT_QPS and K8S_CLIENT_BURST.
// Unset or invalid uses 400 and 800. Values must be > 0.
// Burst may be lower than QPS; do not reorder them.
func kubeClientLimits(qpsRaw, burstRaw string) (qps float32, burst int) {
	qps = defaultKubeClientQPS
	burst = defaultKubeClientBurst

	if v, ok := parsePositiveFloat32(qpsRaw); ok {
		qps = v
	}
	if v, ok := parsePositiveInt(burstRaw); ok {
		burst = v
	}
	return qps, burst
}

func parsePositiveFloat32(s string) (float32, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 32)
	if err != nil || f <= 0 {
		return 0, false
	}
	return float32(f), true
}

func parsePositiveInt(s string) (int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
