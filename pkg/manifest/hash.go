package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ywacch/cloud9/pkg/deployer"
)

type hashDesign struct {
	Version        string     `json:"version"`
	Template       string     `json:"template"`
	Archetype      string     `json:"archetype"`
	Nodes          []hashNode `json:"nodes"`
	Connections    []hashConn `json:"connections"`
	SessionMinutes *int       `json:"sessionMinutes,omitempty"`
}

type hashNode struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

type hashConn struct {
	SourceID string `json:"sourceId"`
	TargetID string `json:"targetId"`
	Protocol string `json:"protocol"`
}

func hashView(d deployer.DeploymentDesign) hashDesign {
	out := hashDesign{
		Version:        d.Version,
		Template:       d.Template,
		Archetype:      d.Archetype,
		SessionMinutes: d.SessionMinutes,
		Nodes:          make([]hashNode, 0, len(d.Nodes)),
		Connections:    make([]hashConn, 0, len(d.Connections)),
	}
	for _, n := range d.Nodes {
		cfg := map[string]interface{}{}
		for k, v := range n.Config {
			cfg[k] = v
		}
		out.Nodes = append(out.Nodes, hashNode{
			ID:     n.ID,
			Type:   n.Type,
			Config: cfg,
		})
	}
	for _, c := range d.Connections {
		out.Connections = append(out.Connections, hashConn{
			SourceID: c.SourceID,
			TargetID: c.TargetID,
			Protocol: c.Protocol,
		})
	}
	sort.Slice(out.Nodes, func(i, j int) bool {
		return out.Nodes[i].ID < out.Nodes[j].ID
	})
	sort.Slice(out.Connections, func(i, j int) bool {
		if out.Connections[i].SourceID != out.Connections[j].SourceID {
			return out.Connections[i].SourceID < out.Connections[j].SourceID
		}
		if out.Connections[i].TargetID != out.Connections[j].TargetID {
			return out.Connections[i].TargetID < out.Connections[j].TargetID
		}
		return out.Connections[i].Protocol < out.Connections[j].Protocol
	})
	return out
}

// DesignHash returns a stable hash of the parts of a design that change behavior.
func DesignHash(d deployer.DeploymentDesign) (string, error) {
	copy := d
	deployer.NormalizeDesign(&copy)
	view := hashView(copy)
	b, err := json.Marshal(view)
	if err != nil {
		return "", fmt.Errorf("marshal hash view: %w", err)
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
