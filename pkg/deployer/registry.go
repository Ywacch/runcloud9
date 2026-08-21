package deployer

import (
	corev1 "k8s.io/api/core/v1"
)

// ExporterSpec defines a Prometheus metrics sidecar injected next to the main container.
type ExporterSpec struct {
	Name        string
	Image       string
	Port        int32
	MetricsPath string
	Env         map[string]string
}

// ComponentSpec describes a simple infrastructure component.
// The deployer creates a Deployment + optional Service from this spec.
type ComponentSpec struct {
	AppLabel     string
	Image        string
	Ports        []int32
	ServiceName  string
	ServicePort  int32
	DefaultEnv   map[string]string
	NeedsService bool
	Resources    corev1.ResourceRequirements
	Annotations  map[string]string
	Exporter     *ExporterSpec
	Command      []string // optional: overrides container entrypoint+cmd
}

const (
	imageRedis     = "redis:7"
	imageNginx     = "nginx:alpine"
	imageNginxFull = "nginx:latest"
	imageBusybox   = "busybox:latest"
)

// componentRegistry maps node types to their infrastructure specs.
// Only simple, off-the-shelf components go here. Complex types (cache, app-server,
// worker, lb) use explicit deploy functions — see lb.go for LBSpec.
var componentRegistry = map[string]ComponentSpec{
	"db": {
		AppLabel:    "postgres",
		Image:       "postgres:15",
		Ports:       []int32{5432},
		ServiceName: "db",
		ServicePort: 5432,
		// Keep docker-entrypoint so init/db creation still runs (Command replaces image CMD).
		Command: []string{
			"docker-entrypoint.sh",
			"postgres",
			"-c", "shared_preload_libraries=pg_stat_statements",
			"-c", "max_replication_slots=10",
			"-c", "wal_level=replica",
			"-c", "max_wal_senders=10",
			"-c", "hot_standby=on",
		},
		DefaultEnv: map[string]string{
			"POSTGRES_DB":               "syscrack",
			"POSTGRES_USER":             "syscrack",
			"POSTGRES_PASSWORD":         "syscrack",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		NeedsService: true,
		Exporter: &ExporterSpec{
			Name:        "postgres-exporter",
			Image:       "quay.io/prometheuscommunity/postgres-exporter:v0.16.0",
			Port:        9187,
			MetricsPath: "/metrics",
			Env: map[string]string{
				"DATA_SOURCE_NAME": "postgresql://syscrack:syscrack@localhost:5432/syscrack?sslmode=disable",
			},
		},
	},
	"mq": {
		AppLabel:    "rabbitmq",
		Image:       "rabbitmq:3-alpine",
		Ports:       []int32{5672, 15692},
		ServiceName: "mq",
		ServicePort: 5672,
		Command: []string{
			"sh", "-c",
			"rabbitmq-plugins enable --offline rabbitmq_prometheus && exec docker-entrypoint.sh rabbitmq-server",
		},
		DefaultEnv: map[string]string{
			"RABBITMQ_DEFAULT_USER": "cloud9",
			"RABBITMQ_DEFAULT_PASS": "cloud9",
		},
		NeedsService: true,
	},
}
