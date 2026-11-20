package placement

// ArchitectureGuardAllowlist lists per-file hits for role-from-type patterns (E-23a baseline).
var ArchitectureGuardAllowlist = map[string]int{
	"pkg/api/introspect.go":                 1,
	"pkg/deployer/design_normalize.go":      1,
	"pkg/deployer/job_bindings.go":          2,
	"pkg/deployer/placement_layouts.go":     1,
	"pkg/deployer/job_bindings_test.go":     1,
	"pkg/deployer/topology_rules.go":        1,
	"pkg/deployer/work_queue_topology.go":   1,
	"pkg/designplan/plan.go":                1,
	"pkg/logs/streamer.go":                  4,
	"pkg/logs/streamer_test.go":             2,
	"pkg/deployer/template_capabilities.go": 1,
	"pkg/templates/registry_test.go":        1,
}
