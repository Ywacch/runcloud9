package metrics

import (
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/placement"
)

// promNamespaceSelector is the standard namespace label filter.
func promNamespaceSelector(ns string) string {
	return fmt.Sprintf(`namespace=%q`, ns)
}

// promAppSelector adds an app label when appLabel is set (per-member PromQL, E-23c).
func promAppSelector(ns, appLabel string) string {
	if appLabel == "" {
		return promNamespaceSelector(ns)
	}
	return fmt.Sprintf(`namespace=%q,app=%q`, ns, appLabel)
}

func namespaceFromPromSel(sel string) string {
	const p = `namespace="`
	if i := strings.Index(sel, p); i >= 0 {
		rest := sel[i+len(p):]
		if j := strings.IndexByte(rest, '"'); j >= 0 {
			return rest[:j]
		}
	}
	return ""
}

func jobComponentAppLabel(topo *placement.Topology, component string) string {
	if topo == nil {
		return ""
	}
	for _, j := range topo.Jobs {
		if j.Component != component {
			continue
		}
		if m, ok := topo.PrimaryMember(j.NodeID); ok {
			return m.AppLabel
		}
		return ""
	}
	return ""
}
