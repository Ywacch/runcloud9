package metrics

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var narrativePlaceholder = regexp.MustCompile(`\{([a-zA-Z0-9_.]+)\}`)

// interpolateNarrativePlaceholders replaces `{metric.key}` tokens using keys from
// dm.TemplateMetrics (e.g. `{worker.fanout_size_p99}`). Unknown keys stay literal.
func interpolateNarrativePlaceholders(s string, dm *DeploymentMetrics) string {
	if s == "" || dm == nil {
		return s
	}
	return narrativePlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		key := match[1 : len(match)-1]
		if dm.TemplateMetrics == nil {
			return match
		}
		v, ok := dm.TemplateMetrics[key]
		if !ok {
			return match
		}
		return formatNarrativeFloat(v)
	})
}

func formatNarrativeFloat(v float64) string {
	if v == 0 {
		return "0"
	}
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	s := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", v), "0"), ".")
	if s == "" || s == "-" {
		return fmt.Sprintf("%g", v)
	}
	return s
}
