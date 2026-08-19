package templates

import (
	"embed"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/supabase-community/supabase-go"
	"github.com/ywacch/cloud9/pkg/introspect"
	"gopkg.in/yaml.v3"
)

//go:embed definitions/*.yaml
var definitionsFS embed.FS

// SeedConfig defines how to seed a template's database after deployment (backend-only).
type SeedConfig struct {
	DBType    string `json:"dbType"`
	DumpPath  string `json:"dumpPath"`
	Container string `json:"container"`
	PodLabel  string `json:"podLabel"`
}

// TemplateSpec is the single source of truth for a template's metadata.
type TemplateSpec struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Description     string          `json:"description"`
	Components      int             `json:"components"`
	Bottleneck      string          `json:"bottleneck"`
	DescriptionLite string          `json:"descriptionLite,omitempty"`
	ComponentsLite  int             `json:"componentsLite,omitempty"`
	BottleneckLite  string          `json:"bottleneckLite,omitempty"`
	Available       bool            `json:"available"`
	Premium         bool            `json:"premium"`
	Tags            []string        `json:"tags"`
	Topology        []TopologyHint  `json:"topology"`
	Preset          *TemplatePreset `json:"preset,omitempty"`
	PresetLite      *TemplatePreset `json:"presetLite,omitempty"`
	AppServerImage  string          `json:"-"`
	WorkerImage     string          `json:"-"`
	Simulatable     bool            `json:"simulatable"`
	Deployable      bool            `json:"deployable"`
	// SupportsWorker is true when the template ships a real worker image (workerImageKey set).
	SupportsWorker      bool             `json:"supportsWorker"`
	SeedConfig          *SeedConfig      `json:"-"`
	ProxyQueries        []ProxyQueryRule `json:"-"`
	AppDatabaseMode     string           `json:"-"`
	WorkerTimelineStore string           `json:"-"`
	// LoadgenDefaultRPS is the suggested default RPS when starting loadgen with no rps in the request (0 = use global 50).
	LoadgenDefaultRPS int `json:"loadgenDefaultRps,omitempty"`
	// Loadgen holds YAML-authored endpoint specs and optional default RPS.
	Loadgen *LoadgenSpec `json:"-"`
	// Storytelling is optional template-authored copy for UI narratives, roles, and journeys.
	Storytelling *StorytellingSpec `json:"storytelling,omitempty"`
	// Introspection configures live DB/cache browsing for the data snapshots UI.
	Introspection introspect.Spec `json:"introspection,omitempty"`
	// PrometheusMetrics defines template-specific PromQL for templateMetrics (from YAML profile or inline).
	PrometheusMetrics *PrometheusMetricsSpec `json:"-"`
	// InvariantCheckers defines YAML-driven L1 checks against templateMetrics keys.
	InvariantCheckers []InvariantRule `json:"-"`
}

// PresetNode is a fully-configured node for canvas population.
type PresetNode struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Position [3]float64     `json:"position"` // x, y, z
	Label    string         `json:"label"`
	Config   map[string]any `json:"config"`
}

// PresetEdge is a directed connection for canvas population.
type PresetEdge struct {
	ID       string `json:"id"`
	SourceID string `json:"sourceId"`
	TargetID string `json:"targetId"`
	Protocol string `json:"protocol"`
}

// TemplatePreset is the full canvas snapshot shipped with the template.
type TemplatePreset struct {
	Nodes []PresetNode `json:"nodes"`
	Edges []PresetEdge `json:"edges"`
}

// TopologyHint is a simplified node for the frontend's mini preview.
type TopologyHint struct {
	Type     string     `json:"type"`
	Position [2]float64 `json:"position"` // x, z for 2D preview
}

// ProxyQueryRule maps cache keys (regex) to SQL for caching-proxy GET miss-fill (always-on in proxy mode).
// Serialized to queries.yaml in cluster; duplicated struct shape in images/caching-proxy.
type ProxyQueryRule struct {
	Pattern string      `json:"-" yaml:"pattern"`
	SQL     string      `json:"-" yaml:"sql"`
	Params  []ParamSpec `json:"-" yaml:"params"`
}

// ParamSpec binds a regex capture group to a SQL placeholder ($1, $2, ...).
type ParamSpec struct {
	Capture int    `json:"-" yaml:"capture"` // 1-indexed capture index
	DBType  string `json:"-" yaml:"dbType"`  // text | integer | boolean
}

// templateDef mirrors embedded YAML before conversion to TemplateSpec.
type templateDef struct {
	ID              string   `yaml:"id"`
	Name            string   `yaml:"name"`
	Description     string   `yaml:"description"`
	Components      int      `yaml:"components"`
	Bottleneck      string   `yaml:"bottleneck"`
	DescriptionLite string   `yaml:"descriptionLite"`
	ComponentsLite  int      `yaml:"componentsLite"`
	BottleneckLite  string   `yaml:"bottleneckLite"`
	Available       bool     `yaml:"available"`
	Premium         bool     `yaml:"premium"`
	Tags            []string `yaml:"tags"`
	Simulatable     bool     `yaml:"simulatable"`
	Deployable      bool     `yaml:"deployable"`
	ImageKey        string   `yaml:"imageKey"`
	WorkerImageKey  string   `yaml:"workerImageKey"`
	Topology        []struct {
		Type     string     `yaml:"type"`
		Position [2]float64 `yaml:"position"`
	} `yaml:"topology"`
	Preset     *presetYAML `yaml:"preset"`
	PresetLite *presetYAML `yaml:"presetLite"`
	Seed       *struct {
		DBType    string `yaml:"dbType"`
		DumpPath  string `yaml:"dumpPath"`
		Container string `yaml:"container"`
		PodLabel  string `yaml:"podLabel"`
	} `yaml:"seed"`
	ProxyQueries         []ProxyQueryRule       `yaml:"proxyQueries"`
	AppDatabaseMode      string                 `yaml:"appDatabaseMode"`
	WorkerTimelineStore  string                 `yaml:"workerTimelineStore"`
	LoadgenDefaultRPS    int                    `yaml:"loadgenDefaultRps"`
	Loadgen              *loadgenDef            `yaml:"loadgen"`
	Storytelling         *StorytellingSpec      `yaml:"storytelling"`
	Introspection        introspect.Spec        `yaml:"introspection"`
	PrometheusMetricsRef string                 `yaml:"prometheusMetricsRef"`
	PrometheusMetrics    *PrometheusMetricsSpec `yaml:"prometheusMetrics"`
	InvariantCheckers    []InvariantRule        `yaml:"invariantCheckers"`
}

type presetYAML struct {
	Nodes []struct {
		ID       string         `yaml:"id"`
		Type     string         `yaml:"type"`
		Position [3]float64     `yaml:"position"`
		Label    string         `yaml:"label"`
		Config   map[string]any `yaml:"config"`
	} `yaml:"nodes"`
	Edges []struct {
		ID       string `yaml:"id"`
		SourceID string `yaml:"sourceId"`
		TargetID string `yaml:"targetId"`
		Protocol string `yaml:"protocol"`
	} `yaml:"edges"`
}

// embeddedFree is the in-repo YAML definitions (always available).
var embeddedFree []TemplateSpec

// loadedTemplates is embedded free templates plus optional Supabase premium rows after InitRegistry.
var loadedTemplates []TemplateSpec

func init() {
	embeddedFree = loadEmbeddedTemplates()
	loadedTemplates = append([]TemplateSpec(nil), embeddedFree...)
}

// InitRegistry merges embedded free templates with premium rows from Supabase.
// Call once at process startup after the Supabase client exists. If client is nil, only free templates load.
func InitRegistry(client *supabase.Client) {
	// Rebuild free templates at InitRegistry time so image tags reflect the latest
	// manifest/env resolution loaded during process startup.
	free := loadEmbeddedTemplates()
	embeddedFree = append([]TemplateSpec(nil), free...)
	premium, err := FetchRemoteTemplates(client)
	if err != nil {
		log.Printf("templates: premium fetch returned error (using free only): %v", err)
		premium = nil
	}
	merged := make([]TemplateSpec, 0, len(free)+len(premium))
	merged = append(merged, free...)
	merged = append(merged, premium...)
	loadedTemplates = merged
	log.Printf("templates: registry loaded %d free + %d premium (%d total)", len(free), len(premium), len(merged))
}

func loadEmbeddedTemplates() []TemplateSpec {
	entries, err := definitionsFS.ReadDir("definitions")
	if err != nil {
		panic("templates: read embedded definitions: " + err.Error())
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]TemplateSpec, 0, len(names))
	for _, name := range names {
		data, err := definitionsFS.ReadFile(path.Join("definitions", name))
		if err != nil {
			panic(fmt.Sprintf("templates: read %s: %v", name, err))
		}
		var def templateDef
		if err := yaml.Unmarshal(data, &def); err != nil {
			panic(fmt.Sprintf("templates: yaml %s: %v", name, err))
		}
		out = append(out, defToSpec(&def))
	}
	// Stable public order (matches historical hardcoded slice; tests rely on it).
	order := map[string]int{
		"url-shortener":    0,
		"ecommerce":        1,
		"social-feed":      2,
		"social-feed-pull": 3,
	}
	sort.SliceStable(out, func(i, j int) bool {
		oi, okI := order[out[i].ID]
		oj, okJ := order[out[j].ID]
		switch {
		case !okI && !okJ:
			return out[i].ID < out[j].ID
		case !okI:
			return false
		case !okJ:
			return true
		default:
			return oi < oj
		}
	})
	return out
}

func defToSpec(def *templateDef) TemplateSpec {
	spec := TemplateSpec{
		ID:                  def.ID,
		Name:                def.Name,
		Description:         def.Description,
		Components:          def.Components,
		Bottleneck:          def.Bottleneck,
		DescriptionLite:     def.DescriptionLite,
		ComponentsLite:      def.ComponentsLite,
		BottleneckLite:      def.BottleneckLite,
		Available:           def.Available,
		Premium:             def.Premium,
		Tags:                def.Tags,
		Simulatable:         def.Simulatable,
		Deployable:          def.Deployable,
		SupportsWorker:      def.WorkerImageKey != "",
		AppServerImage:      imageFor(def.ImageKey),
		Topology:            make([]TopologyHint, 0, len(def.Topology)),
		AppDatabaseMode:     "always",
		WorkerTimelineStore: "none",
	}
	if def.AppDatabaseMode != "" {
		spec.AppDatabaseMode = def.AppDatabaseMode
	}
	if def.WorkerTimelineStore != "" {
		spec.WorkerTimelineStore = def.WorkerTimelineStore
	}
	if def.Loadgen != nil {
		spec.Loadgen = loadgenSpecFromDef(def.Loadgen)
		if def.Loadgen.DefaultRps > 0 {
			spec.LoadgenDefaultRPS = def.Loadgen.DefaultRps
		}
	}
	if spec.LoadgenDefaultRPS == 0 && def.LoadgenDefaultRPS > 0 {
		spec.LoadgenDefaultRPS = def.LoadgenDefaultRPS
	}
	if def.WorkerImageKey != "" {
		spec.WorkerImage = imageFor(def.WorkerImageKey)
	}
	for _, th := range def.Topology {
		spec.Topology = append(spec.Topology, TopologyHint{
			Type:     th.Type,
			Position: th.Position,
		})
	}
	if def.Seed != nil {
		spec.SeedConfig = &SeedConfig{
			DBType:    def.Seed.DBType,
			DumpPath:  def.Seed.DumpPath,
			Container: def.Seed.Container,
			PodLabel:  def.Seed.PodLabel,
		}
	}
	if len(def.ProxyQueries) > 0 {
		spec.ProxyQueries = append([]ProxyQueryRule(nil), def.ProxyQueries...)
	}
	if def.Storytelling != nil {
		// Shallow copy: Journeys, Roles, and NarrativeContext still alias the parsed YAML backing
		// store; safe because template defs are read-only after registry load.
		st := *def.Storytelling
		spec.Storytelling = &st
	}
	if len(def.Introspection) > 0 {
		spec.Introspection = def.Introspection
	}
	if pm := resolvePrometheusMetrics(def); pm != nil {
		spec.PrometheusMetrics = pm
	}
	if len(def.InvariantCheckers) > 0 {
		rules, err := validateInvariantRules(def.InvariantCheckers)
		if err != nil {
			panic(fmt.Sprintf("templates: %s: %v", def.ID, err))
		}
		spec.InvariantCheckers = rules
	}
	if def.Preset != nil {
		spec.Preset = presetFromYAML(def.Preset)
	}
	if def.PresetLite != nil {
		spec.PresetLite = presetFromYAML(def.PresetLite)
	}
	return spec
}

func presetFromYAML(src *presetYAML) *TemplatePreset {
	if src == nil {
		return nil
	}
	preset := &TemplatePreset{
		Nodes: make([]PresetNode, 0, len(src.Nodes)),
		Edges: make([]PresetEdge, 0, len(src.Edges)),
	}
	for _, n := range src.Nodes {
		preset.Nodes = append(preset.Nodes, PresetNode{
			ID:       n.ID,
			Type:     n.Type,
			Position: n.Position,
			Label:    n.Label,
			Config:   n.Config,
		})
	}
	for _, e := range src.Edges {
		preset.Edges = append(preset.Edges, PresetEdge{
			ID:       e.ID,
			SourceID: e.SourceID,
			TargetID: e.TargetID,
			Protocol: e.Protocol,
		})
	}
	return preset
}

func imageFor(name string) string {
	registry := os.Getenv("TEMPLATE_IMAGE_REGISTRY")
	if registry == "" {
		registry = "us-central1-docker.pkg.dev/runcloud9/cloud9-images"
	}
	tag := ImageTag(name)
	return registry + "/" + name + ":" + tag
}

// Registry returns all registered templates (free + optional premium after InitRegistry).
func Registry() []TemplateSpec {
	out := make([]TemplateSpec, len(loadedTemplates))
	copy(out, loadedTemplates)
	return out
}

// ReplaceLoadedTemplatesForTest swaps the active registry and returns a restore function.
func ReplaceLoadedTemplatesForTest(specs []TemplateSpec) func() {
	prev := loadedTemplates
	loadedTemplates = append([]TemplateSpec(nil), specs...)
	return func() {
		loadedTemplates = prev
	}
}

// GetTemplate returns a specific template by ID, or nil if not found.
func GetTemplate(id string) *TemplateSpec {
	for _, t := range loadedTemplates {
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
