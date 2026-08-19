package templates

import (
	"encoding/json"
	"log"

	"github.com/supabase-community/supabase-go"
	"github.com/ywacch/cloud9/pkg/introspect"
)

// cloudTemplateRow matches PostgREST JSON for table cloud_templates (snake_case columns).
// storytelling is jsonb matching StorytellingSpec (roles / edges / journeys / narrativeContext).
// supportsWorker is derived from worker_image_key (same as embedded YAML).
type cloudTemplateRow struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Description       string          `json:"description"`
	Components        int             `json:"components"`
	Bottleneck        string          `json:"bottleneck"`
	Available         bool            `json:"available"`
	Premium           bool            `json:"premium"`
	Tags              []string        `json:"tags"`
	Simulatable       bool            `json:"simulatable"`
	Deployable        bool            `json:"deployable"`
	ImageKey          string          `json:"image_key"`
	WorkerImageKey    string          `json:"worker_image_key"`
	Topology          json.RawMessage `json:"topology"`
	Seed              json.RawMessage `json:"seed"`
	Storytelling      json.RawMessage `json:"storytelling"`
	Loadgen           json.RawMessage `json:"loadgen"`
	PrometheusMetrics json.RawMessage `json:"prometheus_metrics"`
	InvariantCheckers json.RawMessage `json:"invariant_checkers"`
	Preset            json.RawMessage `json:"preset"`
	PresetLite        json.RawMessage `json:"preset_lite"`
	Introspection     json.RawMessage `json:"introspection"`
}

type topologyJSONEntry struct {
	Type     string     `json:"type"`
	Position [2]float64 `json:"position"`
}

type seedJSON struct {
	DBType    string `json:"dbType"`
	DumpPath  string `json:"dumpPath"`
	Container string `json:"container"`
	PodLabel  string `json:"podLabel"`
}

func (row *cloudTemplateRow) toTemplateDef() (*templateDef, error) {
	def := &templateDef{
		ID:             row.ID,
		Name:           row.Name,
		Description:    row.Description,
		Components:     row.Components,
		Bottleneck:     row.Bottleneck,
		Available:      row.Available,
		Premium:        row.Premium,
		Tags:           row.Tags,
		Simulatable:    row.Simulatable,
		Deployable:     row.Deployable,
		ImageKey:       row.ImageKey,
		WorkerImageKey: row.WorkerImageKey,
	}
	if len(row.Topology) > 0 && string(row.Topology) != "null" {
		var tops []topologyJSONEntry
		if err := json.Unmarshal(row.Topology, &tops); err != nil {
			return nil, err
		}
		for _, t := range tops {
			def.Topology = append(def.Topology, struct {
				Type     string     `yaml:"type"`
				Position [2]float64 `yaml:"position"`
			}{Type: t.Type, Position: t.Position})
		}
	}
	if len(row.Seed) > 0 && string(row.Seed) != "null" {
		var s seedJSON
		if err := json.Unmarshal(row.Seed, &s); err != nil {
			return nil, err
		}
		def.Seed = &struct {
			DBType    string `yaml:"dbType"`
			DumpPath  string `yaml:"dumpPath"`
			Container string `yaml:"container"`
			PodLabel  string `yaml:"podLabel"`
		}{
			DBType:    s.DBType,
			DumpPath:  s.DumpPath,
			Container: s.Container,
			PodLabel:  s.PodLabel,
		}
	}
	if len(row.Storytelling) > 0 && string(row.Storytelling) != "null" {
		var story StorytellingSpec
		if err := json.Unmarshal(row.Storytelling, &story); err != nil {
			return nil, err
		}
		def.Storytelling = &story
	}
	if len(row.Loadgen) > 0 && string(row.Loadgen) != "null" {
		var lg loadgenDef
		if err := json.Unmarshal(row.Loadgen, &lg); err != nil {
			return nil, err
		}
		def.Loadgen = &lg
	}
	if len(row.PrometheusMetrics) > 0 && string(row.PrometheusMetrics) != "null" {
		var pm PrometheusMetricsSpec
		if err := json.Unmarshal(row.PrometheusMetrics, &pm); err != nil {
			return nil, err
		}
		def.PrometheusMetrics = &pm
	}
	if len(row.InvariantCheckers) > 0 && string(row.InvariantCheckers) != "null" {
		var rules []InvariantRule
		if err := json.Unmarshal(row.InvariantCheckers, &rules); err != nil {
			return nil, err
		}
		def.InvariantCheckers = rules
	}
	if len(row.Preset) > 0 && string(row.Preset) != "null" {
		var p presetYAML
		if err := json.Unmarshal(row.Preset, &p); err != nil {
			return nil, err
		}
		def.Preset = &p
	}
	if len(row.PresetLite) > 0 && string(row.PresetLite) != "null" {
		var p presetYAML
		if err := json.Unmarshal(row.PresetLite, &p); err != nil {
			return nil, err
		}
		def.PresetLite = &p
	}
	if len(row.Introspection) > 0 && string(row.Introspection) != "null" {
		var intro introspect.Spec
		if err := json.Unmarshal(row.Introspection, &intro); err != nil {
			return nil, err
		}
		def.Introspection = intro
	}
	return def, nil
}

// cloudTemplatesSelect is the PostgREST column list for premium template rows.
const cloudTemplatesSelect = "id,name,description,components,bottleneck,available,premium,tags,simulatable,deployable,image_key,worker_image_key,topology,seed,storytelling,loadgen,prometheus_metrics,invariant_checkers,preset,preset_lite,introspection"

// FetchRemoteTemplates loads premium rows from Supabase cloud_templates.
// On failure (network, missing table, parse error) it logs and returns an empty slice — no error —
// so self-hosted and local runs without premium data keep working.
//
// Requires cloud_templates.storytelling (jsonb, nullable) for edge/role/journey parity with
// embedded YAML. Optional jsonb columns loadgen, prometheus_metrics, and invariant_checkers
// supply premium template load/metrics/invariant config. If storytelling is missing, the select
// fails and premium rows are skipped.
func FetchRemoteTemplates(client *supabase.Client) ([]TemplateSpec, error) {
	if client == nil {
		return []TemplateSpec{}, nil
	}
	data, _, err := client.From("cloud_templates").
		Select(cloudTemplatesSelect, "exact", false).
		Eq("premium", "true").
		Execute()
	if err != nil {
		log.Printf("templates: FetchRemoteTemplates: %v", err)
		return []TemplateSpec{}, nil
	}
	var rows []cloudTemplateRow
	if err := json.Unmarshal(data, &rows); err != nil {
		log.Printf("templates: FetchRemoteTemplates parse: %v", err)
		return []TemplateSpec{}, nil
	}
	out := make([]TemplateSpec, 0, len(rows))
	for i := range rows {
		def, err := rows[i].toTemplateDef()
		if err != nil {
			log.Printf("templates: cloud_templates row %q: %v", rows[i].ID, err)
			continue
		}
		out = append(out, defToSpec(def))
	}
	return out, nil
}
