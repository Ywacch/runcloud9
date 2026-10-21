package loadgen

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/templates"
)

// VUConfig is the closed-loop VU lane configuration on LoadProfile.
type VUConfig = vu.Config

// PodVUConfig is resolved by the API; the loadgen pod never reads template YAML.
type PodVUConfig struct {
	Config  vu.Config            `json:"config"`
	BaseURL string               `json:"baseUrl"`
	Actions map[string]vu.Action `json:"actions"`
	UserIDs []string             `json:"userIds"`
}

// VUActionsFromTemplate copies template VU actions into vu.Action values.
func VUActionsFromTemplate(templateID string) (map[string]vu.Action, bool) {
	spec, ok := templates.LoadgenVU(templateID)
	if !ok || spec == nil || len(spec.Actions) == 0 {
		return nil, false
	}
	out := make(map[string]vu.Action, len(spec.Actions))
	for name, a := range spec.Actions {
		out[name] = vu.Action{
			Method:  a.Method,
			Path:    a.Path,
			Body:    a.Body,
			IDField: a.IDField,
		}
	}
	return out, true
}

func validateVUProfile(p LoadProfile) error {
	if p.VU == nil {
		return nil
	}
	if p.Template == "" {
		return fmt.Errorf("loadgen: vu lane requires a template")
	}
	actions, _ := VUActionsFromTemplate(p.Template)
	if actions == nil {
		actions = map[string]vu.Action{}
	}
	if err := vu.ValidateConfig(vu.DefaultRegistry(), *p.VU, actions); err != nil {
		var ma *vu.MissingActionError
		if errors.As(err, &ma) {
			return fmt.Errorf("loadgen: template %q does not support vu behavior %q", p.Template, ma.Behavior)
		}
		return err
	}
	return nil
}

// NewVURunner builds a runner from a resolved pod VU config.
func NewVURunner(v *PodVUConfig) (*vu.Runner, error) {
	if v == nil {
		return nil, fmt.Errorf("loadgen: vu config is nil")
	}
	return vu.NewRunner(vu.DefaultRegistry(), v.Config, vu.RunnerDeps{
		BaseURL:       v.BaseURL,
		Actions:       v.Actions,
		UserIDs:       v.UserIDs,
		Client:        httpDefaultClient(),
		ApplyIdentity: ApplyClientIdentityHeaders,
	})
}

var vuHTTPClient = defaultVUHTTPClient()

func httpDefaultClient() vu.Doer {
	return vuHTTPClient
}

func defaultVUHTTPClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}
