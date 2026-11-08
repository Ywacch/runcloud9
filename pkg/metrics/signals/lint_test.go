package signals

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

var (
	aggFuncs       = []string{"sum", "max", "min", "avg", "count"}
	metricInExprRe = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\{`)
)

func TestLint_Catalog(t *testing.T) {
	t.Parallel()

	cat := Catalog()
	seen := make(map[ID]struct{}, len(cat))
	for _, s := range cat {
		if s.ID == "" {
			t.Fatal("signal with empty ID")
		}
		if _, dup := seen[s.ID]; dup {
			t.Fatalf("duplicate signal ID %q", s.ID)
		}
		seen[s.ID] = struct{}{}

		if !strings.Contains(s.Expr, "__SEL__") {
			t.Fatalf("signal %q: Expr must contain __SEL__", s.ID)
		}
		if err := lintAggregations(s); err != nil {
			t.Fatalf("signal %q: %v", s.ID, err)
		}
		if err := lintSelectorPlaceholders(s.Expr); err != nil {
			t.Fatalf("signal %q: %v", s.ID, err)
		}

		inExpr := metricsInExpr(s.Expr)
		want := slices.Clone(s.Metrics)
		sort.Strings(want)
		sort.Strings(inExpr)
		if !slices.Equal(want, inExpr) {
			t.Fatalf("signal %q: Metrics %v != names in Expr %v", s.ID, want, inExpr)
		}
	}
}

func lintAggregations(s Signal) error {
	e := s.Expr
	for _, fn := range aggFuncs {
		if strings.Contains(e, fn+"(") && !strings.Contains(e, fn+" by (__BY__") {
			return fmt.Errorf("aggregation %s must use by (__BY__", fn)
		}
	}
	if strings.Contains(e, "histogram_quantile(") && !strings.Contains(e, "by (__BY__") {
		return fmt.Errorf("histogram_quantile must contain an inner by (__BY__ aggregation")
	}
	return nil
}

func lintSelectorPlaceholders(expr string) error {
	i := 0
	for {
		j := strings.Index(expr[i:], "{")
		if j < 0 {
			break
		}
		start := i + j
		end := strings.Index(expr[start:], "}")
		if end < 0 {
			return fmt.Errorf("unclosed selector in Expr")
		}
		block := expr[start : start+end+1]
		if !strings.Contains(block, "__SEL__") {
			return fmt.Errorf("selector %q must contain __SEL__", block)
		}
		i = start + end + 1
	}
	return nil
}

func metricsInExpr(expr string) []string {
	matches := metricInExprRe.FindAllStringSubmatch(expr, -1)
	seen := make(map[string]struct{})
	var out []string
	for _, m := range matches {
		name := m[1]
		if isPromFunc(name) {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func isPromFunc(name string) bool {
	switch name {
	case "rate", "sum", "max", "min", "avg", "count", "histogram_quantile":
		return true
	default:
		return false
	}
}
