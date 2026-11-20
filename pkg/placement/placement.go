// Package placement holds pure topology types shared by deployer, metrics, api, and chaos.
package placement

import "sort"

// Topology is the resolved placement for one deployment design.
type Topology struct {
	Nodes []Node `json:"nodes"`
	Jobs  []Job  `json:"jobs"`
}

// Node is one canvas node with its pods and hosted jobs.
type Node struct {
	NodeID   string   `json:"nodeId"`
	NodeType string   `json:"nodeType"`
	Engine   string   `json:"engine,omitempty"`
	Family   string   `json:"family,omitempty"`
	Members  []Member `json:"members"`
	Jobs     []string `json:"jobs,omitempty"`
}

// Member is one pod or replica serving a node.
type Member struct {
	Role      string `json:"role"`
	Index     int    `json:"index,omitempty"`
	AppLabel  string `json:"appLabel"`
	Container string `json:"container"`
	Service   string `json:"service,omitempty"`
	Port      string `json:"port,omitempty"`
}

// Job is one resolved template job binding.
type Job struct {
	Job       string `json:"job"`
	Component string `json:"component"`
	NodeID    string `json:"nodeId"`
	Engine    string `json:"engine"`
	Shared    bool   `json:"shared,omitempty"`
	Explicit  bool   `json:"explicit,omitempty"`
}

// NodeByID returns the node with the given canvas id.
func (t Topology) NodeByID(id string) (Node, bool) {
	for _, n := range t.Nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return Node{}, false
}

// JobByID returns the placement job row for a template job id.
func (t Topology) JobByID(job string) (Job, bool) {
	for _, j := range t.Jobs {
		if j.Job == job {
			return j, true
		}
	}
	return Job{}, false
}

// NodeForComponent returns the node hosting the first job with the given component alias.
func (t Topology) NodeForComponent(alias string) (Node, bool) {
	for _, j := range t.Jobs {
		if j.Component != alias {
			continue
		}
		return t.NodeByID(j.NodeID)
	}
	return Node{}, false
}

// ComponentsOnNode lists component aliases for jobs hosted on nodeID, sorted.
func (t Topology) ComponentsOnNode(nodeID string) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, j := range t.Jobs {
		if j.NodeID != nodeID {
			continue
		}
		if _, ok := seen[j.Component]; ok {
			continue
		}
		seen[j.Component] = struct{}{}
		out = append(out, j.Component)
	}
	sort.Strings(out)
	return out
}

// PrimaryMember returns the primary member for a node, if any.
func (t Topology) PrimaryMember(nodeID string) (Member, bool) {
	n, ok := t.NodeByID(nodeID)
	if !ok {
		return Member{}, false
	}
	for _, m := range n.Members {
		if m.Role == "primary" || m.Role == "proxy" {
			return m, true
		}
	}
	if len(n.Members) > 0 {
		return n.Members[0], true
	}
	return Member{}, false
}
