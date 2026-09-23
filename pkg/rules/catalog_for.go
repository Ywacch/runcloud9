package rules

import "github.com/ywacch/cloud9/pkg/templates"

// CatalogFor returns the static catalog plus one FamilyInvariant rule per
// template InvariantChecker. Nil tmpl returns the static set only.
func CatalogFor(tmpl *templates.TemplateSpec) []Rule {
	out := All()
	if tmpl == nil || len(tmpl.InvariantCheckers) == 0 {
		return out
	}
	for _, inv := range tmpl.InvariantCheckers {
		summary := inv.Title
		if summary == "" {
			summary = inv.ID
		}
		out = append(out, Rule{
			ID:         inv.ID,
			Family:     FamilyInvariant,
			Kind:       inv.Kind,
			Visibility: VisibilityAll,
			Summary:    summary,
		})
	}
	return out
}
