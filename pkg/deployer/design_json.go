package deployer

import (
	"encoding/json"
	"fmt"
)

// DesignFromMap unmarshals a JSON-compatible map (e.g. DB column) into DeploymentDesign.
func DesignFromMap(m map[string]any) (*DeploymentDesign, error) {
	if m == nil {
		return nil, nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal design map: %w", err)
	}
	var d DeploymentDesign
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("unmarshal design: %w", err)
	}
	return &d, nil
}
