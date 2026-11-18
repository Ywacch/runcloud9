package deployer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDeploy_SessionPodPriorityAboveBalloon(t *testing.T) {
	t.Parallel()

	design := urlShortenerDefaultDesign()
	ns := "cloud9-session-priority"
	fakeClient := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var appNode DeploymentNode
	for _, n := range design.Nodes {
		if n.Type == "app-server" {
			appNode = n
		}
	}

	d := NewK8sDeployer(fakeClient, "app-server")
	_ = d.Deploy(ctx, DeployConfig{Namespace: ns, Node: appNode, Design: design})

	appDep, err := fakeClient.AppsV1().Deployments(ns).Get(context.Background(), "app-server", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get app-server deployment: %v", err)
	}
	if got := appDep.Spec.Template.Spec.PriorityClassName; got != SessionPriorityClassName {
		t.Fatalf("app-server priorityClassName: got %q want %q", got, SessionPriorityClassName)
	}

	balloonValue, sessionValue, balloonDep := loadCloud9BalloonManifest(t)
	if balloonDep.Spec.Template.Spec.PriorityClassName != BalloonPriorityClassName {
		t.Fatalf("balloon deployment priorityClassName: got %q want %q", balloonDep.Spec.Template.Spec.PriorityClassName, BalloonPriorityClassName)
	}
	if sessionValue <= balloonValue {
		t.Fatalf("session PriorityClass value %d must be greater than balloon %d", sessionValue, balloonValue)
	}
}

func loadCloud9BalloonManifest(t *testing.T) (balloonValue, sessionValue int32, balloonDep appsv1.Deployment) {
	t.Helper()

	path := filepath.Join("..", "..", "k8s", "base", "cloud9-balloon.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read balloon manifest: %v", err)
	}

	var foundBalloonPC, foundSessionPC bool
	for _, part := range strings.Split(string(data), "---") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(part), &meta); err != nil {
			t.Fatalf("decode manifest kind: %v", err)
		}
		switch meta.Kind {
		case "PriorityClass":
			var pc schedulingv1.PriorityClass
			if err := yaml.Unmarshal([]byte(part), &pc); err != nil {
				t.Fatalf("unmarshal PriorityClass: %v", err)
			}
			switch pc.Name {
			case BalloonPriorityClassName:
				balloonValue = pc.Value
				foundBalloonPC = true
			case SessionPriorityClassName:
				sessionValue = pc.Value
				foundSessionPC = true
			}
		case "Deployment":
			if err := yaml.Unmarshal([]byte(part), &balloonDep); err != nil {
				t.Fatalf("unmarshal Deployment: %v", err)
			}
		}
	}
	if !foundBalloonPC || !foundSessionPC {
		t.Fatal("balloon manifest missing PriorityClass documents")
	}
	if balloonDep.Name == "" {
		t.Fatal("balloon manifest missing Deployment")
	}
	return balloonValue, sessionValue, balloonDep
}
