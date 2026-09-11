package deployer

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	minAppServerReplicas = 1
	maxAppServerReplicas = 3
	// Workers share the same horizontal scale ceiling as app-servers.
	minWorkerReplicas = minAppServerReplicas
	maxWorkerReplicas = maxAppServerReplicas

	// WarningCodeReadConsistencyClamped is emitted when monotonic policies require
	// at least two read replicas but the design has fewer; deploy clamps to ryw.
	WarningCodeReadConsistencyClamped = "read_consistency_clamped"
	// WarningCodeReplicationSemiSyncEquivalent is emitted when semi-sync is chosen
	// with fewer than two read replicas (ANY 1 vs FIRST 1 is not meaningfully distinct).
	WarningCodeReplicationSemiSyncEquivalent = "replication_semi_sync_equivalent"
)

// ValidateRuntimeContract checks deploy-time runtime preconditions before orchestration starts.
// It complements graph validation by mirroring env derivation and deployer assumptions.
func ValidateRuntimeContract(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	checks := []func(DeploymentDesign, *templates.TemplateSpec) error{
		validateLBBackends,
		validateLBAlgorithm,
		validateAppServerReplicas,
		validateWorkerReplicas,
		validateDBReadReplicas,
		validateDBReplicationConfig,
		validateDBReadConsistency,
		validateProxyQueries,
		validateAppServerEnv,
		validateWorkerEnv,
		validateWorkerTimelineStore,
	}
	for _, check := range checks {
		if err := check(design, tmpl); err != nil {
			return err
		}
	}
	return nil
}

func validateAppServerReplicas(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "app-server" {
			continue
		}
		if _, err := replicaCountFromConfig(node.Config, 1, minAppServerReplicas, maxAppServerReplicas); err != nil {
			return fmt.Errorf("app server %q: %w", node.Name, err)
		}
	}
	return nil
}

func validateWorkerReplicas(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "worker" {
			continue
		}
		if _, err := replicaCountFromConfig(node.Config, 1, minWorkerReplicas, maxWorkerReplicas); err != nil {
			return fmt.Errorf("worker %q: %w", node.Name, err)
		}
	}
	return nil
}

func validateDBReadReplicas(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "db" {
			continue
		}
		if parseReplicaCount(node.Config, 1) > 1 {
			return fmt.Errorf(
				"database %q primary must stay a singleton; use readReplicas for standbys (replicas=%d)",
				node.Name,
				parseReplicaCount(node.Config, 1),
			)
		}
		count, err := readReplicaCountFromConfig(node.Config, minReadReplicas, maxReadReplicas)
		if err != nil {
			return fmt.Errorf("database %q: %w", node.Name, err)
		}
		engine := dbEngineFromConfig(node.Config)
		if count > 0 && !dbEngineSupportsStreamingReplicas(engine) {
			return fmt.Errorf(
				"database %q engine %q does not support read replicas (readReplicas=%d)",
				node.Name, engine, count,
			)
		}
		mode, err := parseReplicationMode(node.Config)
		if err != nil {
			return fmt.Errorf("database %q: %w", node.Name, err)
		}
		if requiresSynchronousReplication(mode) && count < 1 {
			return fmt.Errorf(
				"database %q replicationMode %s requires at least one read replica (readReplicas=%d)",
				node.Name, mode, count,
			)
		}
	}
	return nil
}

func validateDBReplicationConfig(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "db" {
			continue
		}
		if _, err := syncCommitLevelFromConfig(node.Config); err != nil {
			return fmt.Errorf("database %q: %w", node.Name, err)
		}
	}
	return nil
}

func validateDBReadConsistency(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "db" {
			continue
		}
		if _, err := readConsistencyFromConfig(node.Config); err != nil {
			return fmt.Errorf("database %q: %w", node.Name, err)
		}
	}
	return nil
}

// RuntimeContractWarnings returns non-blocking notices for deploy-time bounds adjustments.
func RuntimeContractWarnings(design DeploymentDesign) []TopologyWarning {
	var warnings []TopologyWarning
	for _, node := range design.Nodes {
		if node.Type != "db" {
			continue
		}
		policy, err := readConsistencyFromConfig(node.Config)
		if err != nil {
			continue
		}
		readReplicas := parseReadReplicaCount(node.Config)
		if readReplicas >= 2 || !requiresMultiReplicaReadConsistency(policy) {
			continue
		}
		warnings = append(warnings, TopologyWarning{
			Code: WarningCodeReadConsistencyClamped,
			Message: fmt.Sprintf(
				"Database %q readConsistency %q requires at least two read replicas; deploying with ryw instead",
				node.Name, policy,
			),
			NodeIDs: []string{node.ID},
		})
	}
	for _, node := range design.Nodes {
		if node.Type != "db" {
			continue
		}
		mode, err := parseReplicationMode(node.Config)
		if err != nil {
			continue
		}
		readReplicas := parseReadReplicaCount(node.Config)
		if mode != replicationModeSemiSync || readReplicas >= 2 {
			continue
		}
		warnings = append(warnings, TopologyWarning{
			Code: WarningCodeReplicationSemiSyncEquivalent,
			Message: fmt.Sprintf(
				"Database %q replicationMode semi-sync with one read replica behaves like sync (FIRST 1); add a second replica to teach ANY 1",
				node.Name,
			),
			NodeIDs: []string{node.ID},
		})
	}
	return warnings
}

func validateLBAlgorithm(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "lb" {
			continue
		}
		algo := normalizeLBAlgorithm(configString(node.Config, "lbAlgorithm", lbAlgoRoundRobin))
		if !isLiveLBAlgorithm(algo) {
			return fmt.Errorf(
				"load balancer %q lbAlgorithm must be %s (got %q)",
				node.Name, liveLBAlgorithmList(), algo,
			)
		}
	}
	return nil
}

func validateLBBackends(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "lb" {
			continue
		}
		if len(lbBackends(node, design)) == 0 {
			return fmt.Errorf(
				"load balancer %q has nowhere to send traffic. Connect it to at least one app-server (lb -> app-server)",
				node.Name,
			)
		}
	}
	return nil
}

func validateProxyQueries(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "cache" || !requiresProxy(node) {
			continue
		}
		if err := RequiresProxyQueries(node, tmpl); err != nil {
			return err
		}
	}
	return nil
}

func validateAppServerEnv(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	mode := appDatabaseMode(tmpl)
	for _, node := range design.Nodes {
		if node.Type != "app-server" {
			continue
		}
		env := DeriveEnvVars(node, design)
		if env["DB_HOST"] != "" {
			continue
		}
		if mode == "non-proxy" && env["CACHE_MODE"] == "proxy" {
			continue
		}
		return fmt.Errorf(
			"app server %q still runs database-backed operations in this template, so it needs a direct database connection. Reconnect app-server -> db or switch to a template mode that supports proxy-only app reads (DB_HOST)",
			node.Name,
		)
	}
	return nil
}

func validateWorkerEnv(design DeploymentDesign, _ *templates.TemplateSpec) error {
	for _, node := range design.Nodes {
		if node.Type != "worker" {
			continue
		}
		env := DeriveEnvVars(node, design)
		if env["DB_HOST"] == "" {
			return fmt.Errorf(
				"worker %q needs a database connection to process jobs. Restore worker -> db (DB_HOST)",
				node.Name,
			)
		}
		if workerNeedsMQ(node, design) && env["MQ_HOST"] == "" {
			return fmt.Errorf(
				"worker %q needs a queue connection for this topology. Restore worker -> mq when queue processing is enabled (MQ_HOST)",
				node.Name,
			)
		}
	}
	return nil
}

func workerNeedsMQ(worker DeploymentNode, design DeploymentDesign) bool {
	for _, conn := range design.Connections {
		if conn.SourceID != worker.ID {
			continue
		}
		target := nodeByID(design, conn.TargetID)
		if target != nil && target.Type == "mq" {
			return true
		}
	}
	return false
}

func nodeByID(design DeploymentDesign, id string) *DeploymentNode {
	for _, node := range design.Nodes {
		if node.ID == id {
			n := node
			return &n
		}
	}
	return nil
}

// RequiresProxyQueries ensures a proxy cache strategy has embedded query rules.
func RequiresProxyQueries(cacheNode DeploymentNode, tmpl *templates.TemplateSpec) error {
	if !requiresProxy(cacheNode) {
		return nil
	}
	if tmpl == nil || len(tmpl.ProxyQueries) == 0 {
		templateID := "<unknown>"
		if tmpl != nil && tmpl.ID != "" {
			templateID = tmpl.ID
		}
		return fmt.Errorf(
			"cache strategy on %q needs query rules for cache misses, but template %q does not include them yet. Use cache-aside for this template, or choose one that includes proxy query rules",
			cacheNode.Name,
			templateID,
		)
	}
	return nil
}

func appDatabaseMode(tmpl *templates.TemplateSpec) string {
	if tmpl == nil || tmpl.AppDatabaseMode == "" {
		return "always"
	}
	return tmpl.AppDatabaseMode
}

func validateWorkerTimelineStore(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	if workerTimelineStore(tmpl) != "redis" {
		return nil
	}
	for _, node := range design.Nodes {
		if node.Type != "worker" {
			continue
		}
		env := DeriveEnvVars(node, design)
		if env["CACHE_HOST"] == "" {
			return fmt.Errorf(
				"worker %q needs a cache connection for this template's push timeline store. Add worker -> cache (CACHE_HOST)",
				node.Name,
			)
		}
	}
	return nil
}

func workerTimelineStore(tmpl *templates.TemplateSpec) string {
	if tmpl == nil || tmpl.WorkerTimelineStore == "" {
		return "none"
	}
	return tmpl.WorkerTimelineStore
}
