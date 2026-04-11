package deployer

import (
	"fmt"
	"strings"
)

// ConnectionRule classifies a directed edge (sourceType -> targetType) for semantic validation.
type ConnectionRule struct {
	Valid       bool
	Supported   bool
	Explanation string // set when Valid is false, or when Supported is false (not wired yet)
}

// TopologyAssessment is the outcome of semantic validation plus future deploy/simulate bucketing.
type TopologyAssessment struct {
	Deployable  []DeploymentNode
	Simulatable []DeploymentNode
	Unsupported []DeploymentNode
	Verdict     string // "full" | "partial" | "simulate" | "invalid"
	Explanation string
}

const (
	VerdictFull     = "full"
	VerdictPartial  = "partial"
	VerdictSimulate = "simulate"
	VerdictInvalid  = "invalid"
)

var connectionRules map[[2]string]ConnectionRule

func init() {
	connectionRules = buildConnectionRules()
}

func buildConnectionRules() map[[2]string]ConnectionRule {
	m := make(map[[2]string]ConnectionRule, 36)
	vs := ConnectionRule{Valid: true, Supported: true, Explanation: ""}
	invalid := func(msg string) ConnectionRule {
		return ConnectionRule{Valid: false, Supported: false, Explanation: msg}
	}
	unsup := func(msg string) ConnectionRule {
		return ConnectionRule{Valid: true, Supported: false, Explanation: msg}
	}

	dbPassive := "Databases are passive stores — they don't initiate connections to other components"
	lbToDB := "Load balancers distribute traffic to application servers, not databases directly"
	lbToCache := "Load balancers route HTTP traffic to compute nodes, not cache stores"
	lbToMQ := "Load balancers route HTTP traffic, not message queue connections"
	lbToWorker := "Load balancers route HTTP to app-servers; workers consume from queues"
	cacheToApp := "Cache nodes don't push data to application servers"
	cacheToLB := "Cache nodes don't initiate traffic to load balancers"
	cacheToMQ := "Cache nodes don't publish to message queues"
	cacheToWorker := "Cache nodes don't push data to workers"
	cacheToCache := "Cache-to-cache connections are not supported"
	mqToApp := "Message queues deliver to workers, not back to app-servers (use a separate consumer)"
	mqToDB := "Message queues don't write directly to databases; use a worker"
	mqToLB := "Message queues don't route through load balancers"
	mqToCache := "Message queues don't write to caches directly"
	mqToMQ := "Queue chaining is not supported"
	workerToApp := "Workers don't call back to app-servers"
	workerToLB := "Workers process jobs from queues, they don't route through load balancers"
	workerToWorker := "Worker chaining is not supported"
	appToLB := "App-servers receive traffic from load balancers, they don't push connections to them"
	appToWorker := "App-servers publish to message queues, not directly to workers"

	pair := func(a, b string, r ConnectionRule) {
		m[[2]string{a, b}] = r
	}

	// lb -> *
	pair("lb", "lb", unsup("Chained load balancers are not wired yet"))
	pair("lb", "app-server", vs)
	pair("lb", "db", invalid(lbToDB))
	pair("lb", "cache", invalid(lbToCache))
	pair("lb", "mq", invalid(lbToMQ))
	pair("lb", "worker", invalid(lbToWorker))

	// app-server -> *
	pair("app-server", "lb", invalid(appToLB))
	pair("app-server", "app-server", unsup("Service-to-service calls between app-servers are not wired yet"))
	pair("app-server", "db", vs)
	pair("app-server", "cache", vs)
	pair("app-server", "mq", vs)
	pair("app-server", "worker", invalid(appToWorker))

	// db -> *
	pair("db", "lb", invalid(dbPassive))
	pair("db", "app-server", invalid(dbPassive))
	pair("db", "db", invalid(dbPassive))
	pair("db", "cache", invalid(dbPassive))
	pair("db", "mq", invalid(dbPassive))
	pair("db", "worker", invalid(dbPassive))

	// cache -> *
	pair("cache", "db", vs)
	pair("cache", "lb", invalid(cacheToLB))
	pair("cache", "app-server", invalid(cacheToApp))
	pair("cache", "cache", invalid(cacheToCache))
	pair("cache", "mq", invalid(cacheToMQ))
	pair("cache", "worker", invalid(cacheToWorker))

	// mq -> *
	pair("mq", "worker", vs)
	pair("mq", "lb", invalid(mqToLB))
	pair("mq", "app-server", invalid(mqToApp))
	pair("mq", "db", invalid(mqToDB))
	pair("mq", "cache", invalid(mqToCache))
	pair("mq", "mq", invalid(mqToMQ))

	// worker -> *
	pair("worker", "db", vs)
	pair("worker", "cache", vs)
	pair("worker", "mq", unsup("Republishing from workers to the queue is not wired yet"))
	pair("worker", "lb", invalid(workerToLB))
	pair("worker", "app-server", invalid(workerToApp))
	pair("worker", "worker", invalid(workerToWorker))

	return m
}

func isSemanticComponentType(t string) bool {
	switch t {
	case "lb", "app-server", "db", "cache", "mq", "worker":
		return true
	default:
		return false
	}
}

func dedupeExplanations(msgs []string) []string {
	seen := make(map[string]struct{}, len(msgs))
	out := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		msg = strings.TrimSpace(msg)
		if msg == "" {
			continue
		}
		if _, ok := seen[msg]; ok {
			continue
		}
		seen[msg] = struct{}{}
		out = append(out, msg)
	}
	return out
}

// AssessTopology validates connection semantics and runs whole-design ambiguity checks.
// Canvas-only nodes such as "client" (traffic source) are ignored for edge rules.
func AssessTopology(design DeploymentDesign) TopologyAssessment {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}

	unsupportedNodeIDs := make(map[string]struct{})
	var unsupportedReasons []string

	for _, conn := range design.Connections {
		src, ok := nodeMap[conn.SourceID]
		if !ok {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: fmt.Sprintf("unknown source node id %q in connections", conn.SourceID),
			}
		}
		tgt, ok := nodeMap[conn.TargetID]
		if !ok {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: fmt.Sprintf("unknown target node id %q in connections", conn.TargetID),
			}
		}

		if src.Type == "client" || tgt.Type == "client" {
			continue
		}

		if !isSemanticComponentType(src.Type) {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: fmt.Sprintf("unsupported component type %q on connection source", src.Type),
			}
		}
		if !isSemanticComponentType(tgt.Type) {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: fmt.Sprintf("unsupported component type %q on connection target", tgt.Type),
			}
		}

		key := [2]string{src.Type, tgt.Type}
		rule, ok := connectionRules[key]
		if !ok {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: fmt.Sprintf("unsupported connection %s → %s", src.Type, tgt.Type),
			}
		}
		if !rule.Valid {
			return TopologyAssessment{
				Verdict:     VerdictInvalid,
				Explanation: rule.Explanation,
			}
		}
		if !rule.Supported {
			msg := rule.Explanation
			if msg == "" {
				msg = fmt.Sprintf("connection %s → %s is valid but not supported yet", src.Type, tgt.Type)
			}
			unsupportedNodeIDs[src.ID] = struct{}{}
			unsupportedNodeIDs[tgt.ID] = struct{}{}
			unsupportedReasons = append(unsupportedReasons, msg)
		}
	}

	var hasCache, hasDB, hasMQ, hasWorker bool
	for _, n := range design.Nodes {
		switch n.Type {
		case "cache":
			hasCache = true
		case "db":
			hasDB = true
		case "mq":
			hasMQ = true
		case "worker":
			hasWorker = true
		}
	}

	if hasCache && !hasDB {
		return TopologyAssessment{
			Verdict: VerdictInvalid,
			Explanation: "A cache node is present but no database — clarify whether Redis is your primary store or add a database; " +
				"for now this topology is rejected",
		}
	}
	if hasWorker && !hasMQ {
		return TopologyAssessment{
			Verdict:     VerdictInvalid,
			Explanation: "Workers consume from message queues — add a message queue to the design",
		}
	}
	if hasMQ && !hasWorker {
		return TopologyAssessment{
			Verdict:     VerdictInvalid,
			Explanation: "Message queue has no consumer — add a worker node",
		}
	}

	if len(unsupportedReasons) == 0 {
		deployable := make([]DeploymentNode, len(design.Nodes))
		copy(deployable, design.Nodes)
		return TopologyAssessment{
			Deployable:  deployable,
			Simulatable: nil,
			Unsupported: nil,
			Verdict:     VerdictFull,
			Explanation: "All components deployable",
		}
	}

	uniq := dedupeExplanations(unsupportedReasons)
	joined := strings.Join(uniq, "; ")

	var deployableNodes, simulatableNodes []DeploymentNode
	for _, n := range design.Nodes {
		if _, hit := unsupportedNodeIDs[n.ID]; hit {
			simulatableNodes = append(simulatableNodes, n)
		} else {
			deployableNodes = append(deployableNodes, n)
		}
	}

	if len(deployableNodes) > 0 && len(simulatableNodes) > 0 {
		return TopologyAssessment{
			Deployable:  deployableNodes,
			Simulatable: simulatableNodes,
			Unsupported: simulatableNodes,
			Verdict:     VerdictPartial,
			Explanation: fmt.Sprintf(
				"Some connections use features not yet supported for deployment: %s. These components can be simulated instead.",
				joined,
			),
		}
	}
	return TopologyAssessment{
		Deployable:  nil,
		Simulatable: simulatableNodes,
		Unsupported: simulatableNodes,
		Verdict:     VerdictSimulate,
		Explanation: fmt.Sprintf(
			"This design cannot be fully deployed yet: %s. Use simulation to explore the topology.",
			joined,
		),
	}
}
