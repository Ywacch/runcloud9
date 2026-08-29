package api

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/diagnostics"
)

// provisionOrchestrationTimeout is how long Deploy/seed may run once a doorway
// slot is held. Queue wait for MAX_CONCURRENT_PROVISIONS is not included.
func provisionOrchestrationTimeout(deployerEnv string) time.Duration {
	if deployerEnv == "k8s" {
		// Seed + CHECKPOINT + pg_basebackup of a seeded primary (url-shortener ~1M rows)
		// exceeded 5m while replica inits waited on a spread checkpoint.
		return 8 * time.Minute
	}
	return 2 * time.Minute
}

// runBackgroundProvision finishes create-deployment after the HTTP response:
// stagger → wait for doorway → orchestration deadline → orchestrate → status.
func (h *Handler) runBackgroundProvision(
	design deployer.DeploymentDesign,
	namespace, userID, quotaTier string,
	sessionMins int,
) {
	// Stagger starts to avoid K8s API burst throttling. Deployment is already
	// persisted, so the client already has provisioning feedback.
	stagger := time.Duration(rand.Intn(2000)) * time.Millisecond
	time.Sleep(stagger)

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	h.setProvisionCancel(namespace, lifeCancel)
	defer h.clearProvisionCancel(namespace)

	start := time.Now()
	workCtx, cancel, release, err := beginProvisionWork(
		lifeCtx,
		h.door,
		namespace,
		provisionOrchestrationTimeout(os.Getenv("DEPLOYER")),
		func() {
			h.emitProvisionStatus(
				namespace,
				"waiting_for_provision_slot",
				map[string]string{},
				"pending",
				loadgenStatusPending(h.clientset),
				nil,
				"layer_deploying",
			)
		},
	)
	if h.door != nil {
		if err != nil {
			log.Printf("provision door acquire failed namespace=%s waited=%s: %v", namespace, time.Since(start), err)
		} else {
			log.Printf("provision door acquired namespace=%s waited=%s", namespace, time.Since(start))
		}
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || h.isDeploymentStatus(namespace, "terminated") {
			log.Printf("orchestration canceled for namespace=%s", namespace)
			h.wakeDeployQueue()
			return
		}
		h.failProvision(design, namespace, userID, err)
		return
	}
	holdStart := time.Now()
	defer func() {
		h.door.ObserveHold(time.Since(holdStart))
		release()
		cancel()
	}()

	result, err := h.orchestrate(workCtx, design, namespace, quotaTier)
	if err != nil {
		if errors.Is(err, context.Canceled) || h.isDeploymentStatus(namespace, "terminated") {
			log.Printf("orchestration canceled for namespace=%s", namespace)
			h.wakeDeployQueue()
			return
		}
		h.failProvision(design, namespace, userID, err)
		return
	}

	runExpiresAt := time.Now().Add(time.Duration(sessionMins) * time.Minute)
	if h.isDeploymentStatus(namespace, "terminated") {
		log.Printf("skip final running status for terminated namespace=%s", namespace)
		h.provisionETA.Observe(time.Since(start))
		h.wakeDeployQueue()
		return
	}
	if h.store != nil {
		_ = h.store.UpdateStatus(context.Background(), namespace, "running")
		_ = h.store.UpdateExpiresAt(context.Background(), namespace, runExpiresAt)
		_ = h.store.UpdateMetricsSummary(context.Background(), namespace, map[string]any{
			"seeded":     result.Seeded,
			"running_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
	if h.scheduler != nil {
		h.scheduler.Schedule(namespace, runExpiresAt)
	}
	h.provisionETA.Observe(time.Since(start))
	h.wakeDeployQueue()
}

func (h *Handler) failProvision(design deployer.DeploymentDesign, namespace, userID string, err error) {
	log.Printf("orchestration failed for namespace=%s: %v", namespace, err)
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("template", design.Template)
		scope.SetTag("namespace", namespace)
		if userID != "" {
			scope.SetUser(sentry.User{ID: userID})
		}
		sentry.CaptureException(err)
	})
	if h.store != nil {
		_ = h.store.UpdateStatus(context.Background(), namespace, "failed")
	}
	h.wakeDeployQueue()
	if h.diagnostics != nil {
		var createdAt string
		var runningAt string
		durationSeconds := 0
		if h.store != nil {
			if row, rowErr := h.store.GetDeployment(context.Background(), namespace); rowErr == nil {
				createdAt = row.CreatedAt.UTC().Format(time.RFC3339)
				durationSeconds = int(time.Since(row.CreatedAt).Seconds())
				if raw, ok := row.MetricsSummary["running_at"].(string); ok {
					runningAt = raw
				}
			}
		}
		diagCtx, diagCancel := context.WithTimeout(context.Background(), 20*time.Second)
		url, signatures, partial, diagErr := h.diagnostics.Collect(diagCtx, diagnostics.CollectParams{
			Namespace:        namespace,
			Template:         design.Template,
			Outcome:          "failed",
			OrcError:         err.Error(),
			CollectionReason: "failure",
			CreatedAt:        createdAt,
			RunningAt:        runningAt,
			DurationSeconds:  durationSeconds,
		})
		diagCancel()
		if diagErr != nil {
			log.Printf("DDC: collection failed for %s: %v", namespace, diagErr)
			h.writeDiagnosticsSummary(namespace, "", "failed", "failure", signatures, partial, diagErr)
		} else {
			h.writeDiagnosticsSummary(namespace, url, "failed", "failure", signatures, partial, nil)
			log.Printf("DDC: collected for %s -> %s", namespace, url)
		}
	}
	failedTeardownAt := time.Now().Add(30 * time.Second)
	if h.scheduler != nil {
		h.scheduler.Schedule(namespace, failedTeardownAt)
		return
	}
	go func(ns string) {
		time.Sleep(30 * time.Second)
		d, ok := h.deployers["db"]
		if !ok {
			d = &deployer.LocalDeployer{NodeType: "db"}
		}
		if tdErr := d.Teardown(context.Background(), ns); tdErr != nil {
			log.Printf("failed deployment teardown failed for namespace=%s: %v", ns, tdErr)
		}
	}(namespace)
}

// attachWaitFields sets queuePosition and estimatedWaitSeconds for room queue
// or doorway waiters. Live compute only — not persisted to metrics_summary.
func (h *Handler) attachWaitFields(resp map[string]any, ctx context.Context, namespace, status string) {
	if status == statusQueued {
		if pos, eta, ok := h.queuePositionAndETA(ctx, namespace); ok {
			resp["queuePosition"] = pos
			resp["estimatedWaitSeconds"] = eta
		}
		return
	}
	if pos, eta, ok := h.door.Estimate(namespace); ok {
		resp["queuePosition"] = pos
		resp["estimatedWaitSeconds"] = eta
	}
}
