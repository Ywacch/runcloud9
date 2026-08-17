package loadgen

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/templates"
)

// ActorPool names a segment pool used by actor params.
type ActorPool struct {
	Name         string `json:"name" yaml:"name"`
	PoolEndpoint string `json:"poolEndpoint" yaml:"poolEndpoint"`
	PoolField    string `json:"poolField" yaml:"poolField"`
}

// ActorParam describes a tunable actor parameter from template YAML.
type ActorParam struct {
	Key     string      `json:"key" yaml:"key"`
	Type    string      `json:"type" yaml:"type"`
	Default float64     `json:"default" yaml:"default"`
	Min     *float64    `json:"min,omitempty" yaml:"min,omitempty"`
	Max     *float64    `json:"max,omitempty" yaml:"max,omitempty"`
	Pools   []ActorPool `json:"pools,omitempty" yaml:"pools,omitempty"`
	ApplyTo []string    `json:"applyTo,omitempty" yaml:"applyTo,omitempty"`
}

func actorParamsFromTemplate(templateID string) ([]ActorParam, bool) {
	src, ok := templates.LoadgenParams(templateID)
	if !ok {
		return nil, false
	}
	out := make([]ActorParam, len(src))
	for i, p := range src {
		out[i] = actorParamFromTemplate(p)
	}
	return out, true
}

func actorParamFromTemplate(p templates.LoadgenActorParam) ActorParam {
	out := ActorParam{
		Key:     p.Key,
		Type:    p.Type,
		Default: p.Default,
		Min:     p.Min,
		Max:     p.Max,
		ApplyTo: append([]string(nil), p.ApplyTo...),
	}
	for _, pool := range p.Pools {
		out.Pools = append(out.Pools, ActorPool{
			Name:         pool.Name,
			PoolEndpoint: pool.PoolEndpoint,
			PoolField:    pool.PoolField,
		})
	}
	return out
}

func mergeActorParamValues(templateID string, overrides map[string]float64) (map[string]float64, error) {
	defs, ok := actorParamsFromTemplate(templateID)
	if !ok {
		if len(overrides) > 0 {
			return nil, fmt.Errorf("loadgen: template %q does not declare actor params", templateID)
		}
		return nil, nil
	}
	byKey := make(map[string]ActorParam, len(defs))
	for _, d := range defs {
		byKey[d.Key] = d
	}
	for key := range overrides {
		if _, ok := byKey[key]; !ok {
			return nil, fmt.Errorf("loadgen: unknown actor param %q", key)
		}
	}
	out := make(map[string]float64, len(defs))
	for key, def := range byKey {
		value := def.Default
		if v, ok := overrides[key]; ok {
			value = v
		}
		if err := validateActorValue(def, value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func validateActorValue(def ActorParam, value float64) error {
	switch def.Type {
	case "percent":
		if value < 0 || value > 100 {
			return fmt.Errorf("loadgen: actor param %q must be in [0, 100]", def.Key)
		}
	case "int":
		if value != float64(int64(value)) {
			return fmt.Errorf("loadgen: actor param %q must be an integer", def.Key)
		}
	}
	if def.Min != nil && value < *def.Min {
		return fmt.Errorf("loadgen: actor param %q below minimum %v", def.Key, *def.Min)
	}
	if def.Max != nil && value > *def.Max {
		return fmt.Errorf("loadgen: actor param %q above maximum %v", def.Key, *def.Max)
	}
	return nil
}
