package deployer

import "github.com/ywacch/cloud9/pkg/templates"

// LayoutFixtureNames are the layouts the placement tests exercise.
var LayoutFixtureNames = []string{
	"social-feed-wq-rmq",
	"social-feed-wq-share",
	"social-feed-wq-dedicated",
	"social-feed-tl-pg-wq-rmq",
	"social-feed-tl-pg-wq-redis",
	"ecommerce-wq-rmq",
	"ecommerce-wq-share",
	"ecommerce-wq-dedicated",
	"ecommerce-hc-mc-wq-rmq",
	"ecommerce-hc-mc-wq-redis",
	"url-shortener-hc-redis",
	"url-shortener-hc-mc",
	"url-shortener-proxy",
	"social-feed-pull-hc-redis",
	"url-shortener-db-2-replicas",
}

// LayoutDesign returns a deployment design for a named layout fixture.
func LayoutDesign(name string) (DeploymentDesign, bool) {
	tmplSF := templates.GetTemplate("social-feed")
	tmplEC := templates.GetTemplate("ecommerce")
	tmplUS := templates.GetTemplate("url-shortener")
	tmplPull := templates.GetTemplate("social-feed-pull")
	if tmplSF == nil || tmplEC == nil || tmplUS == nil || tmplPull == nil {
		return DeploymentDesign{}, false
	}

	switch name {
	case "social-feed-wq-rmq":
		d := baseSocialFeed(tmplSF)
		appendNodeIfMissing(&d, DeploymentNode{ID: "mq-1", Type: "mq", Name: "queue", Config: map[string]any{"engine": "rabbitmq"}})
		d.JobBindings = map[string]string{"work_queue": "mq-1"}
		return d, true
	case "social-feed-wq-share":
		d := baseSocialFeed(tmplSF)
		d.JobBindings = map[string]string{"timeline": "cache-1", "work_queue": "cache-1"}
		return d, true
	case "social-feed-wq-dedicated":
		d := baseSocialFeedTwoCache(tmplSF)
		d.JobBindings = map[string]string{"timeline": "cache-tl", "work_queue": "cache-wq"}
		return d, true
	case "social-feed-tl-pg-wq-rmq":
		d := baseSocialFeed(tmplSF)
		appendNodeIfMissing(&d, DeploymentNode{ID: "mq-1", Type: "mq", Name: "queue", Config: map[string]any{"engine": "rabbitmq"}})
		d.JobBindings = map[string]string{"timeline": "db-1", "work_queue": "mq-1"}
		return d, true
	case "social-feed-tl-pg-wq-redis":
		d := baseSocialFeedTwoCache(tmplSF)
		d.JobBindings = map[string]string{"timeline": "db-1", "work_queue": "cache-wq"}
		return d, true
	case "ecommerce-wq-rmq":
		d := baseEcommerce(tmplEC)
		d.JobBindings = map[string]string{"work_queue": "mq-1"}
		return d, true
	case "ecommerce-wq-share":
		d := baseEcommerce(tmplEC)
		d.JobBindings = map[string]string{"hot_cache": "cache-1", "work_queue": "cache-1"}
		return d, true
	case "ecommerce-wq-dedicated":
		d := baseEcommerce(tmplEC)
		appendNodeIfMissing(&d, DeploymentNode{ID: "cache-wq", Type: "cache", Name: "queue", Config: map[string]any{"engine": "redis"}})
		d.JobBindings = map[string]string{"hot_cache": "cache-1", "work_queue": "cache-wq"}
		return d, true
	case "ecommerce-hc-mc-wq-rmq":
		d := baseEcommerce(tmplEC)
		setNodeConfig(&d, "cache-1", map[string]any{"engine": "memcached"})
		d.JobBindings = map[string]string{"hot_cache": "cache-1", "work_queue": "mq-1"}
		return d, true
	case "ecommerce-hc-mc-wq-redis":
		d := baseEcommerce(tmplEC)
		setNodeConfig(&d, "cache-1", map[string]any{"engine": "memcached"})
		appendNodeIfMissing(&d, DeploymentNode{ID: "cache-wq", Type: "cache", Name: "queue", Config: map[string]any{"engine": "redis"}})
		d.JobBindings = map[string]string{"hot_cache": "cache-1", "work_queue": "cache-wq"}
		return d, true
	case "url-shortener-hc-redis":
		d := deploymentDesignFromTemplatePreset("url-shortener", tmplUS.Preset)
		return d, true
	case "url-shortener-hc-mc":
		d := deploymentDesignFromTemplatePreset("url-shortener", tmplUS.Preset)
		for i := range d.Nodes {
			if d.Nodes[i].Type == "cache" {
				d.Nodes[i].Config = map[string]any{"engine": "memcached"}
			}
		}
		return d, true
	case "url-shortener-proxy":
		d := deploymentDesignFromTemplatePreset("url-shortener", tmplUS.Preset)
		for i := range d.Nodes {
			if d.Nodes[i].Type == "cache" {
				d.Nodes[i].Config = map[string]any{"engine": "redis", "cacheStrategy": "read-through"}
			}
		}
		return d, true
	case "social-feed-pull-hc-redis":
		d := deploymentDesignFromTemplatePreset("social-feed-pull", tmplPull.Preset)
		appendNodeIfMissing(&d, DeploymentNode{ID: "cache-1", Type: "cache", Name: "cache", Config: map[string]any{"engine": "redis", "cacheStrategy": "cache-aside"}})
		d.JobBindings = map[string]string{"hot_cache": "cache-1"}
		return d, true
	case "url-shortener-db-2-replicas":
		d := deploymentDesignFromTemplatePreset("url-shortener", tmplUS.Preset)
		for i := range d.Nodes {
			if d.Nodes[i].Type == "db" {
				d.Nodes[i].Config = map[string]any{"engine": "postgresql", "readReplicas": 2}
			}
		}
		return d, true
	default:
		return DeploymentDesign{}, false
	}
}

func baseSocialFeed(tmpl *templates.TemplateSpec) DeploymentDesign {
	d := deploymentDesignFromTemplatePreset("social-feed", tmpl.Preset)
	if len(d.Nodes) == 0 {
		return d
	}
	d.Nodes = append([]DeploymentNode(nil), d.Nodes...)
	// Ensure one timeline-store cache node exists for default timeline binding.
	for i := range d.Nodes {
		if d.Nodes[i].Type == "cache" {
			d.Nodes[i].Config = map[string]any{"engine": "redis", "cacheStrategy": "timeline-store"}
			d.Nodes[i].ID = "cache-1"
			break
		}
	}
	return d
}

func baseSocialFeedTwoCache(tmpl *templates.TemplateSpec) DeploymentDesign {
	d := baseSocialFeed(tmpl)
	renameNode(&d, "cache-1", "cache-tl")
	appendNodeIfMissing(&d, DeploymentNode{ID: "cache-wq", Type: "cache", Name: "queue", Config: map[string]any{"engine": "redis"}})
	return d
}

func appendNodeIfMissing(d *DeploymentDesign, n DeploymentNode) {
	for _, existing := range d.Nodes {
		if existing.ID == n.ID {
			return
		}
	}
	d.Nodes = append(d.Nodes, n)
}

func setNodeConfig(d *DeploymentDesign, id string, cfg map[string]any) {
	for i := range d.Nodes {
		if d.Nodes[i].ID == id {
			d.Nodes[i].Config = cfg
			return
		}
	}
}

func renameNode(d *DeploymentDesign, from, to string) {
	for i := range d.Nodes {
		if d.Nodes[i].ID == from {
			d.Nodes[i].ID = to
		}
	}
	for i := range d.Connections {
		if d.Connections[i].SourceID == from {
			d.Connections[i].SourceID = to
		}
		if d.Connections[i].TargetID == from {
			d.Connections[i].TargetID = to
		}
	}
}

func baseEcommerce(tmpl *templates.TemplateSpec) DeploymentDesign {
	d := deploymentDesignFromTemplatePreset("ecommerce", tmpl.Preset)
	return d
}

func deploymentDesignFromTemplatePreset(templateID string, preset *templates.TemplatePreset) DeploymentDesign {
	if preset == nil {
		return DeploymentDesign{Version: "1.0", Template: templateID, Archetype: "standard"}
	}
	d := DeploymentDesign{
		Version:   "1.0",
		Template:  templateID,
		Archetype: "standard",
		Nodes:     make([]DeploymentNode, 0, len(preset.Nodes)),
	}
	for _, n := range preset.Nodes {
		cfg := map[string]interface{}{}
		for k, v := range n.Config {
			cfg[k] = v
		}
		pos := make([]float64, len(n.Position))
		for i, v := range n.Position {
			pos[i] = v
		}
		d.Nodes = append(d.Nodes, DeploymentNode{
			ID:       n.ID,
			Type:     canvasPresetTypeToBackend(n.Type),
			Name:     n.Label,
			Config:   cfg,
			Position: pos,
		})
	}
	for _, e := range preset.Edges {
		d.Connections = append(d.Connections, DeploymentConn{
			SourceID: e.SourceID,
			TargetID: e.TargetID,
			Protocol: e.Protocol,
		})
	}
	NormalizeDesign(&d)
	return d
}

func canvasPresetTypeToBackend(canvasType string) string {
	switch canvasType {
	case "compute":
		return "app-server"
	case "database":
		return "db"
	case "message_queue":
		return "mq"
	case "load_balancer":
		return "lb"
	default:
		return canvasType
	}
}
