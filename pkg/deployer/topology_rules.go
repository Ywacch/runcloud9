package deployer

import (
	"fmt"
	"sort"
	"strings"
)

// Semantic component types used by connection rules and topology validation.
var semanticComponentTypes = []string{
	"lb", "app-server", "db", "cache", "mq", "worker",
}

// Platform message templates shared by ValidateTopology, AssessTopology, and
// GET /meta/topology-rules. Placeholders: {count}, {name}.
const (
	msgSingletonAppServer =
		"only one app-server node is supported (found {count}) — scale with replicas on a single app-server instead of placing multiple nodes"
	msgSingletonWorker =
		"only one worker node is supported (found {count}) — scale with replicas on a single worker instead of placing multiple nodes"
	msgSingletonCache =
		"only one cache node is supported (found {count}) — remove extra cache nodes; the platform provisions a single Redis service"
	msgSingletonDB =
		"only one db node is supported (found {count}) — remove extra database nodes; the platform provisions a single Postgres service"
	msgSingletonMQ =
		"only one mq node is supported (found {count}) — remove extra message queue nodes; the platform provisions a single queue service"

	msgPresenceCacheWithoutDB =
		"A cache node is present but no database — clarify whether Redis is your primary store or add a database; " +
			"for now this topology is rejected"
	msgPresenceWorkerWithoutMQ =
		"Workers consume from message queues — add a message queue to the design"
	msgPresenceMQWithoutWorker =
		"Message queue has no consumer — add a worker node"

	msgWorkerMissingDB =
		`worker "{name}" must connect to a db node (add worker -> db edge)`
	msgWorkerMissingMQ =
		`worker "{name}" must connect to an mq node (add worker -> mq edge)`
	msgWorkerWrongMQDirection =
		`invalid worker/mq edge direction for worker "{name}": found mq -> worker; use worker -> mq`
)

// platformSingleton is one create-blocking single-instance role.
// Order is validation / honesty priority. lb is intentionally omitted — multi-lb
// is required for the chained lb→lb simulation_recommended path.
type platformSingleton struct {
	NodeType          string
	Message           string
	ScaleWithReplicas bool
}

// platformSingletons is the canonical table for ValidateTopology and GET /meta/topology-rules.
var platformSingletons = []platformSingleton{
	{NodeType: "app-server", Message: msgSingletonAppServer, ScaleWithReplicas: true},
	{NodeType: "worker", Message: msgSingletonWorker, ScaleWithReplicas: true},
	{NodeType: "cache", Message: msgSingletonCache, ScaleWithReplicas: false},
	{NodeType: "db", Message: msgSingletonDB, ScaleWithReplicas: false},
	{NodeType: "mq", Message: msgSingletonMQ, ScaleWithReplicas: false},
}

// ExportedConnectionRule is one directed pair in GET /meta/topology-rules.
// Meaning/storytelling strings are intentionally omitted — those live on templates.
type ExportedConnectionRule struct {
	Source      string `json:"source"`
	Target      string `json:"target"`
	Valid       bool   `json:"valid"`
	Supported   bool   `json:"supported"`
	Explanation string `json:"explanation"`
}

// PresenceRule is a whole-design presence check (AssessTopology ambiguity).
type PresenceRule struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// SingletonRule limits how many nodes of a type may appear on the canvas.
// Message uses {count} for the observed node count.
// ScaleWithReplicas is true when FE may offer merge-into-replicas collapse.
type SingletonRule struct {
	NodeType          string `json:"nodeType"`
	Message           string `json:"message"`
	ScaleWithReplicas bool   `json:"scaleWithReplicas"`
}

// RequiredOutboundRule declares required directed deps from a node type.
// Messages use {name} for the node display name.
type RequiredOutboundRule struct {
	NodeType                string   `json:"nodeType"`
	Targets                 []string `json:"targets"`
	MissingDBMessage        string   `json:"missingDbMessage"`
	MissingMQMessage        string   `json:"missingMqMessage"`
	WrongMQDirectionMessage string   `json:"wrongMqDirectionMessage"`
}

// TopologyRulesResponse is returned by GET /meta/topology-rules.
type TopologyRulesResponse struct {
	ComponentTypes   []string                 `json:"componentTypes"`
	Connections      []ExportedConnectionRule `json:"connections"`
	Presence         []PresenceRule           `json:"presence"`
	Singletons       []SingletonRule          `json:"singletons"`
	RequiredOutbound []RequiredOutboundRule   `json:"requiredOutbound"`
}

// NewTopologyRulesResponse builds the public platform-rules payload from the
// same connectionRules map and singleton table used by create validation.
func NewTopologyRulesResponse() TopologyRulesResponse {
	conns := make([]ExportedConnectionRule, 0, len(connectionRules))
	for key, rule := range connectionRules {
		conns = append(conns, ExportedConnectionRule{
			Source:      key[0],
			Target:      key[1],
			Valid:       rule.Valid,
			Supported:   rule.Supported,
			Explanation: rule.Explanation,
		})
	}
	sort.Slice(conns, func(i, j int) bool {
		if conns[i].Source != conns[j].Source {
			return conns[i].Source < conns[j].Source
		}
		return conns[i].Target < conns[j].Target
	})

	singletons := make([]SingletonRule, 0, len(platformSingletons))
	for _, s := range platformSingletons {
		singletons = append(singletons, SingletonRule{
			NodeType:          s.NodeType,
			Message:           s.Message,
			ScaleWithReplicas: s.ScaleWithReplicas,
		})
	}

	types := append([]string(nil), semanticComponentTypes...)
	return TopologyRulesResponse{
		ComponentTypes: types,
		Connections:    conns,
		Presence: []PresenceRule{
			{ID: "cache_without_db", Message: msgPresenceCacheWithoutDB},
			{ID: "worker_without_mq", Message: msgPresenceWorkerWithoutMQ},
			{ID: "mq_without_worker", Message: msgPresenceMQWithoutWorker},
		},
		Singletons: singletons,
		RequiredOutbound: []RequiredOutboundRule{
			{
				NodeType:                "worker",
				Targets:                 []string{"db", "mq"},
				MissingDBMessage:        msgWorkerMissingDB,
				MissingMQMessage:        msgWorkerMissingMQ,
				WrongMQDirectionMessage: msgWorkerWrongMQDirection,
			},
		},
	}
}

func formatTopologyMessage(tmpl string, repl map[string]string) string {
	out := tmpl
	for k, v := range repl {
		out = strings.ReplaceAll(out, "{"+k+"}", v)
	}
	return out
}

func singletonMessage(nodeType string, count int) string {
	for _, s := range platformSingletons {
		if s.NodeType == nodeType {
			return formatTopologyMessage(s.Message, map[string]string{
				"count": fmt.Sprintf("%d", count),
			})
		}
	}
	return fmt.Sprintf("only one %s node is supported (found %d)", nodeType, count)
}

// validatePlatformSingletons rejects designs with more than one node of a
// platform-singleton type (fixed K8s Service/Deployment names).
func validatePlatformSingletons(design DeploymentDesign) error {
	counts := make(map[string]int, len(platformSingletons))
	for _, n := range design.Nodes {
		counts[n.Type]++
	}
	for _, s := range platformSingletons {
		if c := counts[s.NodeType]; c > 1 {
			return fmt.Errorf("%s", singletonMessage(s.NodeType, c))
		}
	}
	return nil
}

func workerMissingDBMessage(name string) string {
	return formatTopologyMessage(msgWorkerMissingDB, map[string]string{"name": name})
}

func workerMissingMQMessage(name string) string {
	return formatTopologyMessage(msgWorkerMissingMQ, map[string]string{"name": name})
}

func workerWrongMQDirectionMessage(name string) string {
	return formatTopologyMessage(msgWorkerWrongMQDirection, map[string]string{"name": name})
}
