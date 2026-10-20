package chaos

import (
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/rules"
	"k8s.io/client-go/kubernetes"
)

type scenarioRegistration struct {
	scenario string
	engine   string // empty = engine-agnostic
	targets  []string
	fidelity string // real | simulated | compressed; empty means real
	new      func(client kubernetes.Interface) ChaosScenario
}

// ScenarioFidelity reports the fidelity tag for a registered scenario.
// Unknown scenarios return real.
func ScenarioFidelity(scenario string) string {
	for _, reg := range scenarioRegistry {
		if reg.scenario != scenario {
			continue
		}
		if reg.fidelity == "" {
			return rules.FidelityReal
		}
		return reg.fidelity
	}
	return rules.FidelityReal
}

var scenarioRegistry = []scenarioRegistration{
	{
		scenario: "pod_kill",
		engine:   "",
		targets:  []string{"app-server", "cache", "db", "mq", "worker", "lb"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PodKillScenario{Client: client}
		},
	},
	{
		scenario: "cache_flush",
		engine:   "redis",
		targets:  []string{"cache"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &CacheFlushScenario{Client: client}
		},
	},
	{
		scenario: scenarioStallReplica,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PartitionScenario{Client: client, Scenario: scenarioStallReplica}
		},
	},
	{
		scenario: scenarioSplitPrimaryReplica,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PartitionScenario{Client: client, Scenario: scenarioSplitPrimaryReplica}
		},
	},
	{
		scenario: scenarioSplitAppStore,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PartitionScenario{Client: client, Scenario: scenarioSplitAppStore}
		},
	},
	{
		scenario: scenarioSplitAppStore,
		engine:   "redis",
		targets:  []string{"cache"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PartitionScenario{Client: client, Scenario: scenarioSplitAppStore}
		},
	},
	{
		scenario: scenarioPromoteReplica,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PromoteScenario{Client: client, Prefer: deployer.PreferMostCurrent}
		},
	},
	{
		scenario: scenarioPromoteLaggingReplica,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &PromoteScenario{Client: client, Prefer: deployer.PreferMostLagging}
		},
	},
	{
		scenario: ScenarioKillReplica,
		engine:   "postgresql",
		targets:  []string{"db"},
		new: func(client kubernetes.Interface) ChaosScenario {
			return &KillReplicaScenario{Client: client}
		},
	},
}

// Resolve constructs a scenario for the given name, target component, and deployment design.
// Engine is derived from the design via deployer.StorytellingTopologyFromDesign.
func Resolve(name string, targetComponent string, design deployer.DeploymentDesign, client kubernetes.Interface) (ChaosScenario, error) {
	regs := registrationsForScenario(name)
	if len(regs) == 0 {
		return nil, fmt.Errorf("chaos: unknown scenario %q", name)
	}
	if targetComponent == "" {
		return nil, fmt.Errorf("chaos: targetComponent is required")
	}

	engine := engineForTarget(targetComponent, design)
	reg, ok := matchRegistration(regs, targetComponent, engine)
	if !ok {
		return nil, fmt.Errorf(
			"chaos: scenario %q does not support target %q with engine %q; supported: %s",
			name, targetComponent, displayEngine(engine), formatSupportedCombinations(name),
		)
	}
	if client == nil {
		return nil, fmt.Errorf("chaos: kubernetes client is required")
	}
	return reg.new(client), nil
}

func registrationsForScenario(name string) []scenarioRegistration {
	var out []scenarioRegistration
	for _, reg := range scenarioRegistry {
		if reg.scenario == name {
			out = append(out, reg)
		}
	}
	return out
}

func matchRegistration(regs []scenarioRegistration, targetComponent, engine string) (scenarioRegistration, bool) {
	for _, reg := range regs {
		if !targetAllowed(reg.targets, targetComponent) {
			continue
		}
		if reg.engine == "" || strings.EqualFold(reg.engine, engine) {
			return reg, true
		}
	}
	return scenarioRegistration{}, false
}

func targetAllowed(targets []string, targetComponent string) bool {
	for _, t := range targets {
		if t == targetComponent {
			return true
		}
	}
	return false
}

func engineForTarget(targetComponent string, design deployer.DeploymentDesign) string {
	topo := deployer.StorytellingTopologyFromDesign(&design, design.Template)
	switch targetComponent {
	case "db":
		return normalizeDBEngine(topo.DBEngine)
	case "cache":
		return topo.CacheEngine
	default:
		return ""
	}
}

func displayEngine(engine string) string {
	if engine == "" {
		return "*"
	}
	return engine
}

func formatSupportedCombinations(scenario string) string {
	var parts []string
	for _, reg := range scenarioRegistry {
		if reg.scenario != scenario {
			continue
		}
		eng := displayEngine(reg.engine)
		for _, target := range reg.targets {
			parts = append(parts, fmt.Sprintf("%s/%s/%s", reg.scenario, eng, target))
		}
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ", ")
}
