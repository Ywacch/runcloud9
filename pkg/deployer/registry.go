package deployer

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
}

// DefaultResources returns the standard 250m/512Mi resource spec.
func DefaultResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("250m"),
			corev1.ResourceMemory:           resource.MustParse("512Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("250m"),
			corev1.ResourceMemory:           resource.MustParse("512Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
		},
	}
}

// ResourceProfile returns resources for a named profile.
func ResourceProfile(profile string) corev1.ResourceRequirements {
	switch profile {
	case "large":
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("500m"),
				corev1.ResourceMemory:           resource.MustParse("1Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("200Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("500m"),
				corev1.ResourceMemory:           resource.MustParse("1Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("200Mi"),
			},
		}
	case "alpha":
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("125m"),
				corev1.ResourceMemory:           resource.MustParse("256Mi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("125m"),
				corev1.ResourceMemory:           resource.MustParse("256Mi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}
	case "small":
		return corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("125m"),
				corev1.ResourceMemory:           resource.MustParse("256Mi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("125m"),
				corev1.ResourceMemory:           resource.MustParse("256Mi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("100Mi"),
			},
		}
	default: // "medium"
		return DefaultResources()
	}
}

// ExporterResources returns minimal resources for metrics sidecars (GKE Autopilot floors ~50m/52Mi).
func ExporterResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("25m"),
			corev1.ResourceMemory:           resource.MustParse("32Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("50Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("25m"),
			corev1.ResourceMemory:           resource.MustParse("32Mi"),
			corev1.ResourceEphemeralStorage: resource.MustParse("50Mi"),
		},
	}
}

const (
	imageRedis     = "redis:7"
	imageNginx     = "nginx:alpine"
	imageNginxFull = "nginx:latest"
	imageBusybox   = "busybox:latest"
)

// componentRegistry maps node types to their infrastructure specs.
// Only simple, off-the-shelf components go here. Complex types (cache, app-server,
// worker, lb) use explicit deploy functions.
var componentRegistry = map[string]ComponentSpec{
	"db": {
		AppLabel:    "postgres",
		Image:       "postgres:15",
		Ports:       []int32{5432},
		ServiceName: "db",
		ServicePort: 5432,
		DefaultEnv: map[string]string{
			"POSTGRES_DB":       "syscrack",
			"POSTGRES_USER":     "syscrack",
			"POSTGRES_PASSWORD": "syscrack",
		},
		NeedsService: true,
		Resources:    DefaultResources(),
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
		Image:       "rabbitmq:3-management",
		Ports:       []int32{5672, 15672},
		ServiceName: "mq",
		ServicePort: 5672,
		DefaultEnv: map[string]string{
			"RABBITMQ_DEFAULT_USER": "cloud9",
			"RABBITMQ_DEFAULT_PASS": "cloud9",
		},
		NeedsService: true,
		Resources:    DefaultResources(),
	},
}
