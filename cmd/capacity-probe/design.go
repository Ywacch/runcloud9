package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
)

// resolveDesign loads a built-in design by name or a JSON design file.
// Relative paths are resolved relative to baseDir (usually the manifest directory).
func resolveDesign(nameOrPath, baseDir string) (deployer.DeploymentDesign, error) {
	key := strings.TrimSpace(nameOrPath)
	if key == "" {
		return deployer.DeploymentDesign{}, fmt.Errorf("design name is required")
	}
	if d, ok := builtinDesign(key); ok {
		return d, nil
	}
	path := key
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return deployer.DeploymentDesign{}, fmt.Errorf("design %q: %w", nameOrPath, err)
	}
	var d deployer.DeploymentDesign
	if err := json.Unmarshal(raw, &d); err != nil {
		return deployer.DeploymentDesign{}, fmt.Errorf("design %q: parse: %w", nameOrPath, err)
	}
	deployer.NormalizeDesign(&d)
	if strings.TrimSpace(d.Template) == "" {
		return deployer.DeploymentDesign{}, fmt.Errorf("design %q: template is required", nameOrPath)
	}
	if len(d.Nodes) == 0 {
		return deployer.DeploymentDesign{}, fmt.Errorf("design %q: at least one node is required", nameOrPath)
	}
	return d, nil
}

func builtinDesign(name string) (deployer.DeploymentDesign, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "url-shortener-small":
		return urlShortenerSmall(), true
	case "url-shortener-large":
		return urlShortenerLarge(), true
	case "social-feed-small":
		return socialFeedSmall(), true
	default:
		return deployer.DeploymentDesign{}, false
	}
}

func listBuiltinDesigns() []string {
	return []string{"url-shortener-small", "url-shortener-large", "social-feed-small"}
}

func urlShortenerSmall() deployer.DeploymentDesign {
	d := deployer.DeploymentDesign{
		Version:   "1.0",
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]any{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":     "cache-aside",
					"instanceSize": "small",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL", Config: map[string]any{"instanceSize": "small"}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app-1", TargetID: "db-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "cache-1", Protocol: "TCP"},
		},
	}
	deployer.NormalizeDesign(&d)
	return d
}

func urlShortenerLarge() deployer.DeploymentDesign {
	d := deployer.DeploymentDesign{
		Version:   "1.0",
		Template:  "url-shortener",
		Archetype: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{
				ID:     "lb-1",
				Type:   "lb",
				Name:   "Load Balancer",
				Config: map[string]any{"instanceSize": "large"},
			},
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "App Server",
				Config: map[string]any{
					"replicas":     3,
					"instanceSize": "large",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":     "cache-aside",
					"instanceSize": "medium",
				},
			},
			{
				ID:     "db-1",
				Type:   "db",
				Name:   "PostgreSQL",
				Config: map[string]any{"instanceSize": "large"},
			},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "lb-1", TargetID: "app-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "db-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "cache-1", Protocol: "TCP"},
		},
	}
	deployer.NormalizeDesign(&d)
	return d
}

func socialFeedSmall() deployer.DeploymentDesign {
	d := deployer.DeploymentDesign{
		Version:   "1.0",
		Template:  "social-feed",
		Archetype: "social-feed",
		Nodes: []deployer.DeploymentNode{
			{ID: "lb-1", Type: "lb", Name: "Load Balancer", Config: map[string]any{"instanceSize": "small"}},
			{
				ID:   "app-1",
				Type: "app-server",
				Name: "Feed Service",
				Config: map[string]any{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{ID: "mq-1", Type: "mq", Name: "RabbitMQ", Config: map[string]any{"instanceSize": "small"}},
			{
				ID:   "worker-1",
				Type: "worker",
				Name: "Fanout Worker",
				Config: map[string]any{
					"replicas":     1,
					"instanceSize": "small",
				},
			},
			{
				ID:   "cache-1",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":     "timeline-store",
					"instanceSize": "small",
				},
			},
			{ID: "db-1", Type: "db", Name: "PostgreSQL", Config: map[string]any{"instanceSize": "small"}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "lb-1", TargetID: "app-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "mq-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "db-1", Protocol: "TCP"},
			{SourceID: "app-1", TargetID: "cache-1", Protocol: "TCP"},
			{SourceID: "worker-1", TargetID: "mq-1", Protocol: "TCP"},
			{SourceID: "worker-1", TargetID: "db-1", Protocol: "TCP"},
			{SourceID: "worker-1", TargetID: "cache-1", Protocol: "TCP"},
		},
	}
	deployer.NormalizeDesign(&d)
	return d
}
