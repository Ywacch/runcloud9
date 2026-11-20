package api

import (
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/templates"
)

func placementFromDeploymentRow(row *store.DeploymentRow) *placement.Topology {
	if row == nil {
		return nil
	}
	design, err := deployer.DesignFromMap(row.Design)
	if err != nil || design == nil {
		return nil
	}
	tmpl := templates.GetTemplate(row.Template)
	if tmpl == nil {
		tmpl = templates.GetTemplate(design.Template)
	}
	if tmpl == nil {
		return nil
	}
	topo, err := deployer.ResolvePlacement(*design, tmpl)
	if err != nil {
		return nil
	}
	return &topo
}
