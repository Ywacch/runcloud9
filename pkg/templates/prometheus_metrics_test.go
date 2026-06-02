package templates

import (
	"strings"
	"testing"
)

func TestTemplatesEmbedPrometheusMetrics(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"social-feed", "social-feed-pull", "ecommerce", "url-shortener"} {
		tmpl := GetTemplate(id)
		if tmpl == nil || tmpl.PrometheusMetrics == nil {
			t.Fatalf("%s metrics not resolved", id)
		}
		for _, q := range tmpl.PrometheusMetrics.Queries {
			if !strings.Contains(q.Expr, NSSelectorPlaceholder) {
				t.Fatalf("%s query %s missing placeholder", id, q.Key)
			}
		}
	}
}

func TestSocialFeedTemplateHasAugments(t *testing.T) {
	t.Parallel()
	tmpl := GetTemplate("social-feed")
	if tmpl == nil || tmpl.PrometheusMetrics == nil {
		t.Fatal("social-feed metrics not resolved")
	}
	if len(tmpl.PrometheusMetrics.Queries) < 10 {
		t.Fatalf("expected many queries, got %d", len(tmpl.PrometheusMetrics.Queries))
	}
	if len(tmpl.PrometheusMetrics.Augments) == 0 {
		t.Fatal("expected social-feed augments")
	}
}

func TestSocialFeedPullUsesSocialFeedProfile(t *testing.T) {
	t.Parallel()
	pull := GetTemplate("social-feed-pull")
	if pull == nil || pull.PrometheusMetrics == nil {
		t.Fatal("social-feed-pull metrics not resolved")
	}
	if len(pull.PrometheusMetrics.Queries) == 0 {
		t.Fatal("social-feed-pull queries empty")
	}
	if pull.PrometheusMetrics.Queries[0].Key != "posts.total_sync" {
		t.Fatalf("unexpected first key: %s", pull.PrometheusMetrics.Queries[0].Key)
	}
}
