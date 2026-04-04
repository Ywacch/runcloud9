package metrics

import (
	"regexp"
	"strconv"
)

var nginxActiveConnRE = regexp.MustCompile(`Active connections:\s+(\d+)`)

// parseNginxActiveConnections reads the first line of nginx stub_status output.
func parseNginxActiveConnections(body string) float64 {
	m := nginxActiveConnRE.FindStringSubmatch(body)
	if len(m) < 2 {
		return 0
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	return v
}
