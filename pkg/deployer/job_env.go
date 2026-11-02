package deployer

import (
	"strconv"
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
)

var defaultPortsByEngine = map[string]string{
	"postgresql": "5432",
	"redis":      "6379",
	"memcached":  "11211",
	"rabbitmq":   "5672",
}

// cacheListenPort is the TCP port clients use for this cache node.
func cacheListenPort(node DeploymentNode) string {
	if port, ok := defaultPortsByEngine[NodeEngine(node)]; ok {
		return port
	}
	return defaultPortsByEngine["redis"]
}

// ListenPort is the client TCP port for an engine id.
// Chaos and introspection use this table. Do not copy the port literal.
func ListenPort(engine string) (int32, bool) {
	raw, ok := defaultPortsByEngine[strings.ToLower(strings.TrimSpace(engine))]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n <= 0 {
		return 0, false
	}
	return int32(n), true
}

// jobEnv returns per-job ENGINE/HOST/PORT vars for node when it has an outbound
// connection to each bound job node. bindings come from ResolveJobBindings.
func jobEnv(node DeploymentNode, design DeploymentDesign, bindings []Binding) map[string]string {
	if len(bindings) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, b := range bindings {
		if b.NodeID == "" || b.Engine == "" {
			continue
		}
		if !hasOutboundToNode(node.ID, b.NodeID, design) {
			continue
		}
		bound := nodeByID(design, b.NodeID)
		if bound == nil {
			continue
		}
		host := serviceHostForNodeType(bound.Type)
		if host == "" {
			continue
		}
		port, ok := defaultPortsByEngine[b.Engine]
		if !ok {
			continue
		}
		prefix := jobEnvPrefix(b.Job)
		out[prefix+"_ENGINE"] = b.Engine
		out[prefix+"_HOST"] = host
		out[prefix+"_PORT"] = port
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func jobEnvPrefix(jobID string) string {
	return strings.ToUpper(strings.TrimSpace(jobID))
}

func serviceHostForNodeType(nodeType string) string {
	switch nodeType {
	case "db":
		return "db"
	case "cache":
		return "cache"
	case "mq":
		return "mq"
	default:
		return ""
	}
}

func hasOutboundToNode(sourceID, targetID string, design DeploymentDesign) bool {
	for _, c := range design.Connections {
		if c.SourceID == sourceID && c.TargetID == targetID {
			return true
		}
	}
	return false
}

func mergeJobEnvIntoDerive(node DeploymentNode, design DeploymentDesign, env map[string]string) {
	tmpl := templates.GetTemplate(design.Template)
	if tmpl == nil || len(tmpl.Jobs) == 0 {
		return
	}
	bindings := ResolveJobBindings(design, tmpl)
	for k, v := range jobEnv(node, design, bindings) {
		env[k] = v
	}
}
