package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/ywacch/cloud9/pkg/api"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/diagnostics"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/seeder"
	"github.com/ywacch/cloud9/pkg/store"
	"github.com/ywacch/cloud9/pkg/teardown"
	"github.com/ywacch/cloud9/pkg/templates"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// version is set at link time (Dockerfile / CI) for Sentry release tagging.
var version = "dev"

func withCORS(next http.Handler) http.Handler {
	raw := os.Getenv("ALLOWED_ORIGINS")
	if raw == "" {
		raw = "http://localhost:3000"
	}
	allowedSet := make(map[string]bool)
	for _, origin := range strings.Split(raw, ",") {
		o := strings.TrimSpace(origin)
		if o != "" {
			allowedSet[o] = true
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if allowedSet[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
		w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID, X-Cloud9-Proxied")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// buildDeployers returns the deployer map, k8s clientset, and optional metrics clientset.
// os.Getenv returns empty string if var not set.
// Default to local so tests and local dev without minikube still work.
func buildDeployers() (map[string]deployer.ComponentDeployer, kubernetes.Interface, metrics.PodMetricsProvider) {
	if os.Getenv("DEPLOYER") != "k8s" {
		log.Println("Using LocalDeployer (set DEPLOYER=k8s for real K8s)")
		return map[string]deployer.ComponentDeployer{
			"db":         &deployer.LocalDeployer{NodeType: "db"},
			"cache":      &deployer.LocalDeployer{NodeType: "cache"},
			"app-server": &deployer.LocalDeployer{NodeType: "app-server"},
			"lb":         &deployer.LocalDeployer{NodeType: "lb"},
			"mq":         &deployer.LocalDeployer{NodeType: "mq"},
			"worker":     &deployer.LocalDeployer{NodeType: "worker"},
		}, nil, nil
	}

	// KUBECONFIG env var lets you point at a specific config file.
	// Useful when running multiple clusters (minikube + GKE) simultaneously.
	// Falls back to ~/.kube/config if not set, same as kubectl default behaviour.
	kubeconfigPath := os.Getenv("KUBECONFIG")
	// In-cluster (GKE pod): use service account auth via KUBERNETES_SERVICE_HOST.
	// Local/dev: keep kubeconfig behavior for minikube/GKE contexts.
	var (
		config *rest.Config
		err    error
	)
	if kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			log.Fatalf("build kubeconfig from KUBECONFIG: %v", err)
		}
	} else if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("build in-cluster config: %v", err)
		}
	} else {
		kubeconfigPath = filepath.Join(homedir.HomeDir(), ".kube", "config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			log.Fatalf("build kubeconfig: %v", err)
		}
	}
	// Raise client-go rate limits for concurrent deployment orchestration.
	// Default (5 QPS, 10 burst) causes 10-16s delays at 20+ concurrent deploys.
	config.QPS = 50
	config.Burst = 100
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("build clientset: %v", err)
	}
	var metricsClient metrics.PodMetricsProvider
	if mc, err := metricsclient.NewForConfig(config); err != nil {
		log.Printf("metrics client unavailable (container usage Metrics API fallback disabled): %v", err)
	} else {
		metricsClient = &metrics.MetricsClientAdapter{Client: mc}
	}

	log.Println("Using K8sDeployer (minikube/GKE)")
	return map[string]deployer.ComponentDeployer{
		"db":         deployer.NewK8sDeployer(clientset, "db"),
		"cache":      deployer.NewK8sDeployer(clientset, "cache"),
		"app-server": deployer.NewK8sDeployer(clientset, "app-server"),
		"lb":         deployer.NewK8sDeployer(clientset, "lb"),
		"mq":         deployer.NewK8sDeployer(clientset, "mq"),
		"worker":     deployer.NewK8sDeployer(clientset, "worker"),
	}, clientset, metricsClient
}

// recoverDeployments re-registers teardown timers for active deployments on startup.
// If the server restarts mid-deployment, time.AfterFunc timers are gone.
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
	jwtMode := strings.TrimSpace(strings.ToLower(os.Getenv("JWT_VERIFY_MODE")))
	switch jwtMode {
	case "":
		jwtMode = "hybrid"
	case "hybrid", "jwks_only", "hs256_only":
	default:
		log.Fatalf("invalid JWT_VERIFY_MODE %q (expected hybrid, jwks_only, or hs256_only)", jwtMode)
	}
	if os.Getenv("ENV") == "production" {
		if supabaseURL == "" {
			log.Fatal("SUPABASE_URL is required in production")
		}
		if (jwtMode == "hybrid" || jwtMode == "hs256_only") && os.Getenv("SUPABASE_JWT_SECRET") == "" {
			log.Fatalf("SUPABASE_JWT_SECRET must be set in production when JWT_VERIFY_MODE=%s", jwtMode)
		}
	}
	if versionsFile := os.Getenv("IMAGE_VERSIONS_FILE"); versionsFile != "" {
		// Explicit override is operator-controlled; fail fast if invalid/unreadable.
		if err := templates.LoadVersionManifest(versionsFile); err != nil {
			log.Fatalf("failed to load image version manifest %q: %v", versionsFile, err)
		}
		log.Printf("loaded image version manifest from %s", versionsFile)
	} else {
		const defaultVersionsFile = "config/image-versions.json"
		if err := templates.LoadVersionManifest(defaultVersionsFile); err != nil {
			// Non-fatal fallback for safety; tag resolution falls back to env/global default.
			log.Printf("WARNING: failed to load default image version manifest %q: %v", defaultVersionsFile, err)
		} else {
			log.Printf("loaded image version manifest from %s", defaultVersionsFile)
		}
	}

	sentryHTTP := false
	if dsn := os.Getenv("SENTRY_DSN"); dsn != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:              dsn,
			Environment:      os.Getenv("ENV"),
			Release:          "cloud9-api@" + version,
			SampleRate:       1.0,
			TracesSampleRate: 1.0,
			EnableLogs:       true,
		}); err != nil {
			log.Printf("sentry.Init: %v", err)
		} else {
			defer sentry.Flush(2 * time.Second)
			sentryHTTP = true
		}
	}

	deployers, clientset, metricsClient := buildDeployers()

	seedBucketName := os.Getenv("SEED_BUCKET_NAME")
	if seedBucketName == "" {
		// Backward-compatible default: keep using existing seed bucket until migrated.
		seedBucketName = "cloud9-seed-data"
	}
	var seed *seeder.Seeder
	if clientset != nil {
		seed = seeder.NewSeeder(clientset, seedBucketName)
	}

	deploymentStore, err := store.NewDeploymentStore(supabaseURL, supabaseKey)
	if err != nil {
		log.Fatalf("failed to init store: %v", err)
	}

	templates.InitRegistry(deploymentStore.SupabaseClient())

	var lg loadgen.LoadGenerator
	if os.Getenv("DEPLOYER") == "k8s" {
		lg = loadgen.NewPodLoadGenerator(clientset)
	} else {
		lg = loadgen.NewLocalLoadGenerator()
	}
	var metricsCollector *metrics.MetricsCollector
	var diagnosticsCollector *diagnostics.Collector

	// teardownFn uses context.Background() so cleanup is never cut short by request cancel or timeout.
	teardownFn := func(namespace string) {
		// Stop load generator first — prevents requests to a dying namespace
		_ = lg.Stop(context.Background(), namespace)
		ctx := context.Background()
		tpl := ""
		var row store.DeploymentRow
		row, rowErr := deploymentStore.GetDeployment(ctx, namespace)
		if rowErr == nil {
			tpl = row.Template
		}
		summary := make(map[string]any)
		if rowErr == nil && row.MetricsSummary != nil {
			for k, v := range row.MetricsSummary {
				summary[k] = v
			}
		}
		// Collect final metrics snapshot and merge into existing summary (preserves 'seeded' and other fields).
		if metricsCollector != nil {
			design, _ := deployer.DesignFromMap(row.Design)
			topo := api.MetricsTopologyFromDeploymentDesign(design, tpl)
			dm, err := metricsCollector.Collect(ctx, namespace, tpl, topo)
			if err == nil && dm != nil {
				for k, v := range metricsCollector.PeakSummaryForNamespace(namespace, dm) {
					summary[k] = v
				}
				if hist := metricsCollector.DownsampledSessionHistory(namespace); len(hist) > 0 {
					summary["session_history"] = hist
					summary["events_summary"] = metrics.BuildEventsSummary(hist, 10)
				}
				if clh := metricsCollector.ClientLatencyHistoryFor(namespace); len(clh) > 0 {
					summary["client_latency_history"] = clh
				}
				summary["terminated_at"] = time.Now().Format(time.RFC3339)
				if rowErr == nil {
					summary["actual_duration_seconds"] = int(time.Since(row.CreatedAt).Seconds())
				}
			}
		}
		if diagnosticsCollector != nil {
			createdAt := ""
			runningAt := ""
			durationSeconds := 0
			if rowErr == nil {
				createdAt = row.CreatedAt.UTC().Format(time.RFC3339)
				durationSeconds = int(time.Since(row.CreatedAt).Seconds())
				if raw, ok := row.MetricsSummary["running_at"].(string); ok {
					runningAt = raw
				}
			}
			diagCtx, diagCancel := context.WithTimeout(ctx, 20*time.Second)
			url, signatures, partial, diagErr := diagnosticsCollector.Collect(diagCtx, diagnostics.CollectParams{
				Namespace:        namespace,
				Template:         tpl,
				Outcome:          "success",
				CollectionReason: "teardown",
				CreatedAt:        createdAt,
				RunningAt:        runningAt,
				DurationSeconds:  durationSeconds,
			})
			diagCancel()
			signatureCodes := make([]string, 0, len(signatures))
			for _, sig := range signatures {
				signatureCodes = append(signatureCodes, sig.Code)
			}
			diagSummary := map[string]any{
				"url":                url,
				"collected_at":       time.Now().UTC().Format(time.RFC3339),
				"outcome":            "success",
				"schema_version":     2,
				"failure_signatures": signatureCodes,
				"partial":            partial,
				"collection_reason":  "teardown",
			}
			if createdAt != "" || durationSeconds > 0 {
				lifecycle := map[string]any{
					"created_at":       createdAt,
					"duration_seconds": durationSeconds,
				}
				if runningAt != "" {
					lifecycle["running_at"] = runningAt
				}
				diagSummary["lifecycle"] = lifecycle
			}
			if diagErr != nil {
				log.Printf("DDC: teardown collection failed for %s: %v", namespace, diagErr)
				diagSummary["error"] = diagErr.Error()
			}
			summary["diagnostics"] = diagSummary
		}
		if len(summary) > 0 {
			if err := deploymentStore.SetMetricsSummary(ctx, namespace, summary); err != nil {
				log.Printf("auto-teardown metrics summary update failed for %s: %v", namespace, err)
			}
		}
		d := deployers["db"]
		if err := d.Teardown(ctx, namespace); err != nil {
			log.Printf("auto-teardown failed for %s: %v", namespace, err)
		}
		if err := deploymentStore.UpdateStatus(ctx, namespace, "terminated"); err != nil {
			log.Printf("auto-teardown status update failed for %s: %v", namespace, err)
		}
		if metricsCollector != nil {
			metricsCollector.Cleanup(namespace)
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
	debugMetrics := os.Getenv("CLOUD9_DEBUG_METRICS") == "true"
	eventBus := metrics.NewEventBus()
	metricsCollector = metrics.NewMetricsCollector(prometheusURL, lg, clientset, metricsClient, eventBus)
	metricsCollector.SetDebugMetrics(debugMetrics)
	if clientset != nil {
		ddcBucket := os.Getenv("DDC_BUCKET")
		if ddcBucket == "" {
			ddcBucket = "cloud9-diagnostics"
		}
		var err error
		diagnosticsCollector, err = diagnostics.NewCollector(clientset, ddcBucket)
		if err != nil {
			log.Printf("DDC: init failed (disabled): %v", err)
		}
	}

	handler := api.NewHandler(deployers, deploymentStore, scheduler, seed, metricsCollector, diagnosticsCollector, clientset, lg, eventBus)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	recoverDeployments(context.Background(), deploymentStore, scheduler)

	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	log.Printf("Cloud9 API starting on :%s", port)
	root := http.Handler(mux)
	if sentryHTTP {
		root = sentryhttp.New(sentryhttp.Options{}).Handle(root)
	}
	if err := http.ListenAndServe(":"+port, withCORS(root)); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
