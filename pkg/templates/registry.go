package templates

import "os"

// SeedConfig defines how to seed a template's database after deployment (backend-only).
type SeedConfig struct {
	DBType    string `json:"dbType"`
	DumpPath  string `json:"dumpPath"`
	Container string `json:"container"`
	PodLabel  string `json:"podLabel"`
}

// TemplateSpec is the single source of truth for a template's metadata.
type TemplateSpec struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Components     int             `json:"components"`
	Bottleneck     string          `json:"bottleneck"`
	Available      bool            `json:"available"`
	Premium        bool            `json:"premium"`
	Tags           []string        `json:"tags"`
	Topology       []TopologyHint  `json:"topology"`
	AppServerImage string          `json:"-"`
	WorkerImage    string          `json:"-"`
	Simulatable    bool            `json:"simulatable"`
	Deployable     bool            `json:"deployable"`
	SeedConfig     *SeedConfig     `json:"-"`
}

// TopologyHint is a simplified node for the frontend's mini preview.
type TopologyHint struct {
	Type     string     `json:"type"`
	Position [2]float64 `json:"position"` // x, z for 2D preview
}

func imageFor(name string) string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "europe-west1-docker.pkg.dev/ganymede-cloud/cloud9-images"
	}
	tag := ":v1"
	switch name {
	case "url-shortener":
		tag = ":v5"
	}
	return registry + "/" + name + tag
}

// Registry returns all registered templates.
func Registry() []TemplateSpec {
	return []TemplateSpec{
		{
			ID:             "url-shortener",
			Name:           "URL Shortener",
			Description:    "Classic read-heavy cache-aside pattern with PostgreSQL + Redis. Demonstrates cache hit/miss ratios, DB query amplification on cache miss, and strategy tradeoffs.",
			Components:     4,
			Bottleneck:     "Cache miss → DB query amplification",
			Available:      true,
			Premium:        false,
			Tags:           []string{"caching", "read-heavy", "beginner"},
			AppServerImage: imageFor("url-shortener"),
			WorkerImage:    "",
			Simulatable:    true,
			Deployable:     true,
			Topology: []TopologyHint{
				{Type: "load_balancer", Position: [2]float64{0, 0}},
				{Type: "compute", Position: [2]float64{2, 0}},
				{Type: "cache", Position: [2]float64{4, -1}},
				{Type: "database", Position: [2]float64{4, 1}},
			},
			SeedConfig: &SeedConfig{
				DBType:    "postgres",
				DumpPath:  "url-shortener/urls.dump",
				Container: "postgres",
				PodLabel:  "app=postgres",
			},
		},
		{
			ID:             "ecommerce",
			Name:           "E-Commerce",
			Description:    "Multi-server product catalog with shared database and cache layer. Tests load balancer distribution and write contention.",
			Components:     5,
			Bottleneck:     "Write contention on shared database",
			Available:      false,
			Premium:        false,
			Tags:           []string{"multi-server", "write-heavy", "intermediate"},
			AppServerImage: imageFor("ecommerce"),
			WorkerImage:    imageFor("ecommerce-worker"),
			Simulatable:    true,
			Deployable:     false,
			Topology: []TopologyHint{
				{Type: "load_balancer", Position: [2]float64{0, 0}},
				{Type: "compute", Position: [2]float64{2, -1}},
				{Type: "compute", Position: [2]float64{2, 1}},
				{Type: "cache", Position: [2]float64{4, 0}},
				{Type: "database", Position: [2]float64{6, 0}},
			},
		},
		{
			ID:             "social-feed",
			Name:           "Social Feed",
			Description:    "Fan-out architecture with message queue and async worker for feed generation. Demonstrates consumer lag, backpressure, and queue depth under load.",
			Components:     6,
			Bottleneck:     "Consumer lag under high fan-out",
			Available:      false,
			Premium:        false,
			Tags:           []string{"async", "fan-out", "advanced"},
			AppServerImage: imageFor("social-feed"),
			WorkerImage:    imageFor("fanout-worker"),
			Simulatable:    true,
			Deployable:     false,
			Topology: []TopologyHint{
				{Type: "load_balancer", Position: [2]float64{0, 0}},
				{Type: "compute", Position: [2]float64{2, 0}},
				{Type: "message_queue", Position: [2]float64{4, -1}},
				{Type: "worker", Position: [2]float64{6, -1}},
				{Type: "cache", Position: [2]float64{4, 1}},
				{Type: "database", Position: [2]float64{6, 1}},
			},
		},
	}
}

// GetTemplate returns a specific template by ID, or nil if not found.
func GetTemplate(id string) *TemplateSpec {
	for _, t := range Registry() {
		if t.ID == id {
			copy := t
			return &copy
		}
	}
	return nil
}

// GetAppServerImage returns the Docker image for the template's app-server, or "" if unknown.
func GetAppServerImage(templateID string) string {
	t := GetTemplate(templateID)
	if t == nil {
		return ""
	}
	return t.AppServerImage
}

// GetWorkerImage returns the Docker image for the template's worker, or "" if none / unknown.
func GetWorkerImage(templateID string) string {
	t := GetTemplate(templateID)
	if t == nil {
		return ""
	}
	return t.WorkerImage
}

// IsDeployable returns true if the template exists and the backend can provision real K8s resources for it.
func IsDeployable(id string) bool {
	t := GetTemplate(id)
	return t != nil && t.Deployable
}
