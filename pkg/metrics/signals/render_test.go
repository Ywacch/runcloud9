package signals

import (
	"strings"
	"testing"
)

func TestRender_AllCatalogClusterAndNamespace(t *testing.T) {
	t.Parallel()

	for _, s := range Catalog() {
		t.Run(string(s.ID)+"_cluster", func(t *testing.T) {
			t.Parallel()
			got, err := RenderCluster(s)
			if err != nil {
				t.Fatalf("RenderCluster: %v", err)
			}
			if strings.Contains(got, "__SEL__") || strings.Contains(got, "__BY__") {
				t.Fatalf("cluster form still has placeholders: %q", got)
			}
			if !strings.Contains(got, `deployment!=""`) {
				t.Fatalf("cluster form missing deployment selector: %q", got)
			}
		})
		t.Run(string(s.ID)+"_namespace", func(t *testing.T) {
			t.Parallel()
			got, err := RenderNamespace(s, "ns-a")
			if err != nil {
				t.Fatalf("RenderNamespace: %v", err)
			}
			if strings.Contains(got, "__SEL__") || strings.Contains(got, "__BY__") {
				t.Fatalf("namespace form still has placeholders: %q", got)
			}
			if !strings.Contains(got, `namespace="ns-a"`) {
				t.Fatalf("namespace form missing namespace: %q", got)
			}
			if s.BareNamespace {
				if strings.Contains(got, `deployment!=""`) {
					t.Fatalf("bare namespace form must omit deployment selector: %q", got)
				}
			} else if !strings.Contains(got, `deployment!=""`) {
				t.Fatalf("namespace form missing deployment selector: %q", got)
			}
		})
	}
}

func TestRender_AppRequestRatePodBareNamespace(t *testing.T) {
	t.Parallel()

	s := signalByID(t, AppRequestRatePod)
	got, err := RenderNamespace(s, "ns-replica")
	if err != nil {
		t.Fatalf("RenderNamespace: %v", err)
	}
	if strings.Contains(got, `deployment!=""`) {
		t.Fatalf("pod request rate must use bare namespace selector: %q", got)
	}
	if !strings.Contains(got, "by (pod)") {
		t.Fatalf("missing pod group by: %q", got)
	}
	if strings.Contains(got, "by (namespace, pod)") {
		t.Fatalf("namespace form must group by pod only: %q", got)
	}
}

func TestRender_AppRequestRate(t *testing.T) {
	t.Parallel()

	s := signalByID(t, AppRequestRate)
	cluster, err := RenderCluster(s)
	if err != nil {
		t.Fatalf("RenderCluster: %v", err)
	}
	if !strings.Contains(cluster, `deployment!=""`) {
		t.Fatalf("cluster: %q", cluster)
	}
	if !strings.Contains(cluster, "by (namespace, app)") {
		t.Fatalf("cluster missing group by: %q", cluster)
	}

	ns, err := RenderNamespace(s, "ns-a")
	if err != nil {
		t.Fatalf("RenderNamespace: %v", err)
	}
	if !strings.Contains(ns, `namespace="ns-a"`) {
		t.Fatalf("namespace: %q", ns)
	}
	if !strings.Contains(ns, "by (namespace, app)") {
		t.Fatalf("namespace missing group by: %q", ns)
	}
}

func TestRender_RejectsEmptyNamespace(t *testing.T) {
	t.Parallel()

	s := signalByID(t, AppRequestRate)
	if _, err := RenderNamespace(s, ""); err == nil {
		t.Fatal("expected error for empty namespace")
	}
}

func TestRender_LatencyQuantileKeepsLe(t *testing.T) {
	t.Parallel()

	s := signalByID(t, AppLatencyP99)
	cluster, err := RenderCluster(s)
	if err != nil {
		t.Fatalf("RenderCluster: %v", err)
	}
	if !strings.Contains(cluster, "by (namespace, app, le)") {
		t.Fatalf("p99 cluster form must group by le: %q", cluster)
	}
}

func TestRender_NamespaceEscapesQuotes(t *testing.T) {
	t.Parallel()

	s := signalByID(t, AppRequestRate)
	got, err := RenderNamespace(s, `ns"evil`)
	if err != nil {
		t.Fatalf("RenderNamespace: %v", err)
	}
	if !strings.Contains(got, `namespace="ns\"evil"`) {
		t.Fatalf("namespace not escaped: %q", got)
	}
}

func signalByID(t *testing.T, id ID) Signal {
	t.Helper()
	for _, s := range Catalog() {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("signal %q not in catalog", id)
	return Signal{}
}
