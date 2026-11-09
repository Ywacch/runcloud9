package signals

import (
	"errors"
	"fmt"
	"strings"
)

// RenderCluster replaces __SEL__ with deployment!="" and __BY__ from Shape.
func RenderCluster(s Signal) (string, error) {
	return render(s, clusterSelector(), groupBy(s.Shape))
}

// RenderNamespace replaces __SEL__ with deployment!="" and namespace="<ns>", and __BY__ from Shape.
// ns must be non-empty.
func RenderNamespace(s Signal, ns string) (string, error) {
	var sel string
	var err error
	if s.BareNamespace {
		sel, err = bareNamespaceSelector(ns)
	} else {
		sel, err = namespaceSelector(ns)
	}
	if err != nil {
		return "", err
	}
	return render(s, sel, namespaceGroupBy(s))
}

// namespaceGroupBy matches legacy replica PromQL: one namespace is already
// selected, so per-pod series group by pod (and le stays on the histogram expr).
func namespaceGroupBy(s Signal) string {
	if s.BareNamespace && s.Shape == PerPod {
		return "pod"
	}
	return groupBy(s.Shape)
}

func render(s Signal, selector, groupBy string) (string, error) {
	if strings.TrimSpace(s.Expr) == "" {
		return "", errors.New("signals: empty expression")
	}
	out := strings.ReplaceAll(s.Expr, "__SEL__", selector)
	out = strings.ReplaceAll(out, "__BY__", groupBy)
	if strings.Contains(out, "__SEL__") || strings.Contains(out, "__BY__") {
		return "", fmt.Errorf("signals: expression still has placeholders after render")
	}
	return out, nil
}

func groupBy(shape Shape) string {
	switch shape {
	case PerNamespace:
		return "namespace"
	case PerApp:
		return "namespace, app"
	case PerPod:
		return "namespace, pod"
	case PerBucket:
		return "namespace, le"
	default:
		return "namespace"
	}
}

func clusterSelector() string {
	return `deployment!=""`
}

func namespaceSelector(ns string) (string, error) {
	if ns == "" {
		return "", errors.New("signals: namespace is empty")
	}
	escaped := escapeNamespace(ns)
	return fmt.Sprintf(`deployment!="",namespace="%s"`, escaped), nil
}

func bareNamespaceSelector(ns string) (string, error) {
	if ns == "" {
		return "", errors.New("signals: namespace is empty")
	}
	escaped := escapeNamespace(ns)
	return fmt.Sprintf(`namespace="%s"`, escaped), nil
}

func escapeNamespace(ns string) string {
	escaped := strings.ReplaceAll(ns, `\`, `\\`)
	return strings.ReplaceAll(escaped, `"`, `\"`)
}
