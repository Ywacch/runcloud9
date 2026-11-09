package metrics

import (
	"math"
	"os"
	"strings"
)

// SignalCompareObserver records hub vs legacy PromQL comparisons.
type SignalCompareObserver func(signal, result string)

func (c *MetricsCollector) SetSignalCompareObserver(fn SignalCompareObserver) {
	if c == nil {
		return
	}
	c.signalCompareObserver = fn
}

// SignalCompareEnabled reports whether METRICS_SIGNAL_COMPARE runs legacy queries beside signals.
func SignalCompareEnabled(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func signalCompareEnabled() bool {
	return SignalCompareEnabled(os.Getenv("METRICS_SIGNAL_COMPARE"))
}

func promValuesMatch(signalVal, oldVal float64, signalOK, oldOK bool) bool {
	if !signalOK && !oldOK {
		return true
	}
	if signalOK != oldOK {
		return false
	}
	tol := math.Max(0.01, math.Max(math.Abs(signalVal), math.Abs(oldVal))*0.05)
	return math.Abs(signalVal-oldVal) <= tol
}
