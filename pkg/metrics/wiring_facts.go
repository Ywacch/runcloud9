package metrics

import (
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/templates"
)

func appendWiringFacts(l *Ledger, design *deployer.DeploymentDesign) {
	if l == nil || design == nil {
		return
	}
	for _, link := range deployer.WiringLinksFromDesign(design) {
		prefix := fmt.Sprintf("fact:wiring:%s->%s.", link.SourceID, link.TargetID)
		if link.Balancing != "" {
			l.addFact(Fact{
				Ref:       FactRef(prefix + "balancing"),
				Kind:      FactKindWiring,
				Component: link.TargetType,
				Value:     link.Balancing,
			})
		}
		if link.ReadRoutingPolicy != "" {
			l.addFact(Fact{
				Ref:       FactRef(prefix + "readRoutingPolicy"),
				Kind:      FactKindWiring,
				Component: link.TargetType,
				Value:     link.ReadRoutingPolicy,
			})
		}
		keys := sortedStrings(keysOfStringMap(link.Fields))
		for _, key := range keys {
			l.addFact(Fact{
				Ref:       FactRef(prefix + key),
				Kind:      FactKindWiring,
				Component: link.TargetType,
				Value:     link.Fields[key],
			})
		}
	}
}

func appendLoadgenMixFacts(l *Ledger, template string, spec *templates.TemplateSpec) {
	if l == nil {
		return
	}
	template = strings.TrimSpace(template)
	if template == "" && spec != nil {
		template = spec.ID
	}
	endpoints, ok := templates.LoadgenEndpoints(template)
	if !ok {
		return
	}
	for _, ep := range endpoints {
		key := loadgenMixEndpointKey(ep)
		if key == "" {
			continue
		}
		l.addFact(Fact{
			Ref:  FactRef("fact:config:loadgen.mix." + key),
			Kind: FactKindConfig,
			Value: map[string]any{
				"method":       ep.Method,
				"pathTemplate": ep.PathTemplate,
				"weight":       ep.Weight,
			},
		})
	}
}

func loadgenMixEndpointKey(ep templates.LoadgenEndpointSpec) string {
	path := strings.TrimSpace(ep.PathTemplate)
	if path == "" {
		path = strings.TrimSpace(ep.PoolEndpoint)
	}
	path = strings.TrimPrefix(path, "/")
	path = strings.ReplaceAll(path, "/", ".")
	path = strings.ReplaceAll(path, "{", "")
	path = strings.ReplaceAll(path, "}", "")
	return path
}

func keysOfStringMap(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
