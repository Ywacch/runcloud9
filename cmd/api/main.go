package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ywacch/ganymede/pkg/api"
	"github.com/ywacch/ganymede/pkg/deployer"
	"github.com/ywacch/ganymede/pkg/metrics"
	"github.com/ywacch/ganymede/pkg/seeder"
	"github.com/ywacch/ganymede/pkg/store"
	"github.com/ywacch/ganymede/pkg/teardown"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// buildDeployers returns the deployer map. os.Getenv returns empty string if var not set.
// Default to local so tests and local dev without minikube still work.
func buildDeployers() map[string]deployer.ComponentDeployer {
	if os.Getenv("DEPLOYER") != "k8s" {
		log.Println("Using LocalDeployer (set DEPLOYER=k8s for real K8s)")
		return map[string]deployer.ComponentDeployer{
			"db":         &deployer.LocalDeployer{NodeType: "db"},
			"cache":      &deployer.LocalDeployer{NodeType: "cache"},
			"app-server": &deployer.LocalDeployer{NodeType: "app-server"},
			"lb":         &deployer.LocalDeployer{NodeType: "lb"},
			"mq":         &deployer.LocalDeployer{NodeType: "mq"},
		}
	}

	// UNDERSTAND: KUBECONFIG env var lets you point at a specific config file.
	// Useful when running multiple clusters (minikube + GKE) simultaneously.
	// Falls back to ~/.kube/config if not set — same as kubectl default behaviour.
	kubeconfigPath := os.Getenv("KUBECONFIG")
	if kubeconfigPath == "" {
		kubeconfigPath = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		log.Fatalf("build kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("build clientset: %v", err)
	}

	log.Println("Using K8sDeployer (minikube/GKE)")
	return map[string]deployer.ComponentDeployer{
		"db":         deployer.NewK8sDeployer(clientset, "db"),
		"cache":      deployer.NewK8sDeployer(clientset, "cache"),
		"app-server": deployer.NewK8sDeployer(clientset, "app-server"),
		"lb":         &deployer.LocalDeployer{NodeType: "lb"},
		"mq":         &deployer.LocalDeployer{NodeType: "mq"},
	}
}

// recoverDeployments re-registers teardown timers for active deployments on startup.
// UNDERSTAND: if the server restarts mid-deployment, time.AfterFunc timers are gone.
// We scan for running/provisioning deployments and either fire teardown now (if expired)
// or re-register a timer for the remaining duration.
func recoverDeployments(ctx context.Context, s *store.DeploymentStore, sched *teardown.Scheduler) {
	rows, err := s.GetStaleDeployments(ctx)
	if err != nil {
		log.Printf("recovery: failed to fetch stale deployments: %v", err)
		return
	}
	for _, row := range rows {
		if row.ExpiresAt.Before(time.Now()) {
			sched.Schedule(row.Namespace, time.Now())
		} else {
			sched.Schedule(row.Namespace, row.ExpiresAt)
		}
		log.Printf("recovery: registered timer for %s", row.Namespace)
	}
}

func main() {
	supabaseURL := os.Getenv("SUPABASE_URL")
	supabaseKey := os.Getenv("SUPABASE_SERVICE_KEY")
	if supabaseURL == "" || supabaseKey == "" {
		log.Fatal("SUPABASE_URL and SUPABASE_SERVICE_KEY are required")
	}

	deployers := buildDeployers()

	const seedBucketName = "ganymede-seed-data"
	var seed *seeder.Seeder
	if os.Getenv("DEPLOYER") == "k8s" {
		kubeconfigPath := os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			kubeconfigPath = filepath.Join(homedir.HomeDir(), ".kube", "config")
		}
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			log.Fatalf("build kubeconfig: %v", err)
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			log.Fatalf("build clientset: %v", err)
		}
		seed = seeder.NewSeeder(clientset, seedBucketName)
	}

	deploymentStore, err := store.NewDeploymentStore(supabaseURL, supabaseKey)
	if err != nil {
		log.Fatalf("failed to init store: %v", err)
	}

	// teardownFn uses context.Background() so cleanup is never cut short by request cancel or timeout.
	teardownFn := func(namespace string) {
		ctx := context.Background()
		d := deployers["db"]
		if err := d.Teardown(ctx, namespace); err != nil {
			log.Printf("auto-teardown failed for %s: %v", namespace, err)
		}
		if err := deploymentStore.UpdateStatus(ctx, namespace, "terminated"); err != nil {
			log.Printf("auto-teardown status update failed for %s: %v", namespace, err)
		}
		log.Printf("auto-teardown complete: %s", namespace)
	}

	scheduler := teardown.NewScheduler(teardownFn)

	// Prometheus is accessible inside the cluster at this DNS name.
	// kube-prometheus-stack exposes it as a ClusterIP service in the monitoring namespace.
	// When running locally, set PROMETHEUS_URL to a port-forwarded URL or leave empty for zeros.
	prometheusURL := os.Getenv("PROMETHEUS_URL")
	if prometheusURL == "" && os.Getenv("DEPLOYER") == "k8s" {
		prometheusURL = "http://kube-prometheus-stack-prometheus.monitoring.svc.cluster.local:9090"
	}
	metricsCollector := metrics.NewMetricsCollector(prometheusURL)

	handler := api.NewHandler(deployers, deploymentStore, scheduler, seed, metricsCollector)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	recoverDeployments(context.Background(), deploymentStore, scheduler)

	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	log.Printf("Ganymede Cloud API starting on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
