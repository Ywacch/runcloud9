package templates

// LoadgenActorPool names a segment pool fetched via poolEndpoint (may include query string).
type LoadgenActorPool struct {
	Name         string `yaml:"name" json:"name"`
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
}

// LoadgenActorParam declares a tunable loadgen actor parameter from template YAML.
type LoadgenActorParam struct {
	Key     string             `yaml:"key" json:"key"`
	Type    string             `yaml:"type" json:"type"`
	Default float64            `yaml:"default" json:"default"`
	Min     *float64           `yaml:"min,omitempty" json:"min,omitempty"`
	Max     *float64           `yaml:"max,omitempty" json:"max,omitempty"`
	Pools   []LoadgenActorPool `yaml:"pools,omitempty" json:"pools,omitempty"`
	ApplyTo []string           `yaml:"applyTo,omitempty" json:"applyTo,omitempty"`
	// Tier is a closed-product concept; OSS loadgen ignores it.
	Tier string `yaml:"tier,omitempty" json:"-"`
}

type loadgenActorPoolDef struct {
	Name         string `yaml:"name" json:"name"`
	PoolEndpoint string `yaml:"poolEndpoint" json:"poolEndpoint"`
	PoolField    string `yaml:"poolField" json:"poolField"`
}

type loadgenActorParamDef struct {
	Key     string                `yaml:"key" json:"key"`
	Type    string                `yaml:"type" json:"type"`
	Default float64               `yaml:"default" json:"default"`
	Min     *float64              `yaml:"min" json:"min,omitempty"`
	Max     *float64              `yaml:"max" json:"max,omitempty"`
	Pools   []loadgenActorPoolDef `yaml:"pools" json:"pools,omitempty"`
	ApplyTo []string              `yaml:"applyTo" json:"applyTo,omitempty"`
	Tier    string                `yaml:"tier" json:"tier,omitempty"`
}
