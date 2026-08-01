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
)

// ValidateRuntimeContract checks deploy-time runtime preconditions before orchestration starts.
// It complements graph validation by mirroring env derivation and deployer assumptions.
func ValidateRuntimeContract(design DeploymentDesign, tmpl *templates.TemplateSpec) error {
	checks := []func(DeploymentDesign, *templates.TemplateSpec) error{
		validateLBBackends,
		validateLBAlgorithm,
		validateAppServerReplicas,
		validateWorkerReplicas,
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

func validateLBAlgorithm(design DeploymentDesign, _ *templates.TemplateSpec) error {
	allowed := map[string]struct{}{
		"round-robin": {},
		"least_conn":  {},
		"ip_hash":     {},
	}
	for _, node := range design.Nodes {
		if node.Type != "lb" {
			continue
		}
		algo := configString(node.Config, "lbAlgorithm", "round-robin")
		if _, ok := allowed[algo]; !ok {
			return fmt.Errorf(
				"load balancer %q lbAlgorithm must be round-robin, least_conn, or ip_hash (got %q)",
				node.Name, algo,
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
