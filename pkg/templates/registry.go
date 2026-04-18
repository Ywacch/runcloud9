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
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Components     int            `json:"components"`
	Bottleneck     string         `json:"bottleneck"`
	Available      bool           `json:"available"`
	Premium        bool           `json:"premium"`
	Tags           []string       `json:"tags"`
	Topology       []TopologyHint `json:"topology"`
	AppServerImage string         `json:"-"`
	WorkerImage    string         `json:"-"`
	Simulatable    bool           `json:"simulatable"`
	Deployable     bool           `json:"deployable"`
	SeedConfig     *SeedConfig    `json:"-"`
}

// TopologyHint is a simplified node for the frontend's mini preview.
type TopologyHint struct {
	Type     string     `json:"type"`
	Position [2]float64 `json:"position"` // x, z for 2D preview
}

// templateDef mirrors embedded YAML before conversion to TemplateSpec.
type templateDef struct {
	ID             string `yaml:"id"`
	Name           string `yaml:"name"`
	Description    string `yaml:"description"`
	Components     int    `yaml:"components"`
	Bottleneck     string `yaml:"bottleneck"`
	Available      bool   `yaml:"available"`
	Premium        bool   `yaml:"premium"`
	Tags           []string `yaml:"tags"`
	Simulatable    bool     `yaml:"simulatable"`
	Deployable     bool     `yaml:"deployable"`
	ImageKey       string   `yaml:"imageKey"`
	WorkerImageKey string   `yaml:"workerImageKey"`
	Topology       []struct {
		Type     string     `yaml:"type"`
		Position [2]float64 `yaml:"position"`
	} `yaml:"topology"`
	Seed *struct {
		DBType    string `yaml:"dbType"`
		DumpPath  string `yaml:"dumpPath"`
		Container string `yaml:"container"`
		PodLabel  string `yaml:"podLabel"`
	} `yaml:"seed"`
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
	free := append([]TemplateSpec(nil), embeddedFree...)
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
		"url-shortener": 0,
		"ecommerce":     1,
		"social-feed":   2,
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
		ID:             def.ID,
		Name:           def.Name,
		Description:    def.Description,
		Components:     def.Components,
		Bottleneck:     def.Bottleneck,
		Available:      def.Available,
		Premium:        def.Premium,
		Tags:           def.Tags,
		Simulatable:    def.Simulatable,
		Deployable:     def.Deployable,
		AppServerImage: imageFor(def.ImageKey),
		Topology:       make([]TopologyHint, 0, len(def.Topology)),
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
	return spec
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

// Registry returns all registered templates (free + optional premium after InitRegistry).
func Registry() []TemplateSpec {
	out := make([]TemplateSpec, len(loadedTemplates))
	copy(out, loadedTemplates)
	return out
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
