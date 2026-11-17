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
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
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

// provisionIsPaid reports whether a user gets paid-tier doorway and queue priority.
func provisionIsPaid(user store.UserRecord) bool {
	return user.IsInternal || store.ResolveEffectiveTier(user) == store.TierEngineer
}

// provisionStagger returns the pre-door sleep for background provision.
func provisionStagger(paid bool) time.Duration {
	if paid {
		return 0
	}
	return time.Duration(rand.Intn(2000)) * time.Millisecond
}

// runBackgroundProvision finishes create-deployment after the HTTP response:
// stagger → wait for doorway → orchestration deadline → orchestrate → status.
func (h *Handler) runBackgroundProvision(
	design deployer.DeploymentDesign,
	namespace, userID, quotaTier string,
	sessionExpiresAt time.Time,
	sessionMins int,
) {
	var paid bool
	if userID != "" && h.store != nil {
		if userRecord, err := h.store.GetUser(context.Background(), userID); err == nil {
			paid = provisionIsPaid(userRecord)
		}
	}
	time.Sleep(provisionStagger(paid))

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()
	h.setProvisionCancel(namespace, lifeCancel)
	defer h.clearProvisionCancel(namespace)

	start := time.Now()
	var doorWaited bool
	var doorWait time.Duration
	workCtx, cancel, release, err := beginProvisionWork(
		lifeCtx,
		h.door,
		namespace,
		provisionOrchestrationTimeout(os.Getenv("DEPLOYER")),
		paid,
		func() {
			doorWaited = true
			waitStart := time.Now().UTC()
			h.emitProvisionStatus(
				namespace,
				"waiting_for_provision_slot",
				map[string]string{},
				"pending",
				loadgenStatusPending(h.clientset),
				nil,
				"layer_deploying",
			)
			h.stampMetrics(context.Background(), namespace, map[string]any{
				"door_wait_started_at": rfc3339UTC(waitStart),
			})
			h.recordAdmissionEvent(context.Background(), store.AdmissionEvent{
				UserID:    userID,
				Namespace: namespace,
				Template:  design.Template,
				Kind:      store.AdmissionKindDoorWait,
				Paid:      paid,
			})
		},
	)
	if h.door != nil {
		if err != nil {
			log.Printf("provision door acquire failed namespace=%s paid=%t waited=%s: %v", namespace, paid, time.Since(start), err)
		} else {
			log.Printf("provision door acquired namespace=%s paid=%t waited=%s", namespace, paid, time.Since(start))
		}
	}
	if err == nil && doorWaited {
		acquiredAt := time.Now()
		h.stampMetrics(context.Background(), namespace, map[string]any{
			"door_acquired_at": rfc3339UTC(acquiredAt),
		})
		h.recordAdmissionEvent(context.Background(), store.AdmissionEvent{
			UserID:    userID,
			Namespace: namespace,
			Template:  design.Template,
			Kind:      store.AdmissionKindDoorAcquired,
			WaitMS:    intPtr(durationMillis(acquiredAt.Sub(start))),
			Paid:      paid,
		})
	}
	if err == nil {
		waitSec := 0.0
		if doorWaited {
			doorWait = time.Since(start)
			waitSec = doorWait.Seconds()
		}
		if h.cpMetrics != nil {
			h.cpMetrics.observeDoorWait(waitSec)
		}
	} else if doorWaited && h.cpMetrics != nil {
		h.cpMetrics.observeDoorWait(time.Since(start).Seconds())
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || h.provisionWritesBlocked(namespace) {
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

	result, err := h.orchestrate(workCtx, design, namespace, quotaTier, sessionExpiresAt)
	if err != nil {
		if errors.Is(err, context.Canceled) || h.provisionWritesBlocked(namespace) {
			log.Printf("orchestration canceled for namespace=%s", namespace)
			h.wakeDeployQueue()
			return
		}
		h.recordProvisionTimings(context.Background(), namespace, holdStart, doorWait)
		h.failProvision(design, namespace, userID, err)
		return
	}

	runExpiresAt := time.Now().Add(time.Duration(sessionMins) * time.Minute)
	if h.provisionWritesBlocked(namespace) {
		log.Printf("skip final running status for terminated namespace=%s", namespace)
		h.recordProvisionTimings(context.Background(), namespace, holdStart, doorWait)
		h.provisionETA.Observe(time.Since(start))
		h.wakeDeployQueue()
		return
	}
	if h.store != nil {
		claimed, claimErr := h.store.ClaimStatus(context.Background(), namespace, []string{statusProvisioning}, statusRunning)
		if claimErr != nil {
			log.Printf("running status claim failed namespace=%s: %v", namespace, claimErr)
			h.wakeDeployQueue()
			return
		}
		if !claimed {
			log.Printf("skip final running status for namespace=%s", namespace)
			h.recordProvisionTimings(context.Background(), namespace, holdStart, doorWait)
			h.provisionETA.Observe(time.Since(start))
			h.wakeDeployQueue()
			return
		}
		_ = h.store.UpdateExpiresAt(context.Background(), namespace, runExpiresAt)
		h.stampMetrics(context.Background(), namespace, map[string]any{
			"seeded":     result.Seeded,
			"running_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
	if h.scheduler != nil {
		h.scheduler.Schedule(namespace, runExpiresAt)
	}
	if h.metricsCollector != nil {
		topo := MetricsTopologyFromDeploymentDesign(&design, design.Template)
		if h.store != nil {
			row, err := h.store.GetDeployment(context.Background(), namespace)
			if err == nil && row.RunRole != nil && *row.RunRole != "" {
				cmpID := ""
				if row.ComparisonID != nil {
					cmpID = *row.ComparisonID
				}
				h.metricsCollector.SetRunInfo(namespace, metrics.RunInfo{
					Role:         *row.RunRole,
					ComparisonID: cmpID,
				})
			}
		}
		h.metricsCollector.EnsurePublisher(namespace, design.Template, topo)
		h.refreshWebsocketLiveGauge()
	}
	h.recordProvisionTimings(context.Background(), namespace, holdStart, doorWait)
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
		claimed, claimErr := h.store.ClaimStatus(context.Background(), namespace, []string{statusProvisioning}, statusFailed)
		if claimErr != nil {
			log.Printf("failed status claim failed namespace=%s: %v", namespace, claimErr)
		}
		if !claimed {
			log.Printf("skip fail status for namespace=%s", namespace)
			h.wakeDeployQueue()
			return
		}
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
