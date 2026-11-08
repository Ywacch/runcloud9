package api

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/diagnostics"
	"github.com/ywacch/cloud9/pkg/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	teardownReasonDelete = "delete"
	teardownReasonExpiry = "teardown"

	teardownJobBudget         = 4 * time.Minute
	teardownCollectBudget     = 15 * time.Second
	teardownDiagCollectBudget = 20 * time.Second
	teardownSweepEvery        = 30 * time.Second
	orphanNamespaceMinAge     = 5 * time.Minute
)

// statusesThatMayTearDown are the rows a delete or expiry timer may claim.
// terminating and terminated are not in this list. A second caller must not win.
var statusesThatMayTearDown = []string{statusProvisioning, statusRunning, statusFailed}

type teardownJob struct {
	at     time.Time
	reason string
}

// teardownQueue is the process-lifetime pool for namespace deletion.
// DELETE returns after the terminating row is durable. This pool does the rest.
type teardownQueue struct {
	h        *Handler
	workers  int
	diagWait time.Duration
	diagSem  chan struct{}

	mu       sync.Mutex
	pending  map[string]teardownJob
	inflight map[string]struct{}
	wake     chan struct{}
	wg       sync.WaitGroup
}

func newTeardownQueue(h *Handler, workers, diagSlots int, diagWait time.Duration) *teardownQueue {
	if workers < 1 {
		workers = 1
	}
	if diagSlots < 1 {
		diagSlots = 1
	}
	if diagWait < 0 {
		diagWait = 0
	}
	return &teardownQueue{
		h:        h,
		workers:  workers,
		diagWait: diagWait,
		diagSem:  make(chan struct{}, diagSlots),
		pending:  make(map[string]teardownJob),
		inflight: make(map[string]struct{}),
		wake:     make(chan struct{}, 1),
	}
}

func (q *teardownQueue) start() {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.loop()
	}
}

func (q *teardownQueue) enqueue(namespace string, at time.Time, reason string) {
	if q == nil || namespace == "" {
		return
	}
	if reason == "" {
		reason = teardownReasonExpiry
	}
	if at.IsZero() {
		at = time.Now()
	}
	q.mu.Lock()
	if _, busy := q.inflight[namespace]; busy {
		depth, inflight := len(q.pending), len(q.inflight)
		q.mu.Unlock()
		q.h.setTeardownGauges(depth, inflight)
		return
	}
	job := teardownJob{at: at, reason: reason}
	if prev, ok := q.pending[namespace]; ok {
		if at.Before(prev.at) {
			job.at = at
		} else {
			job.at = prev.at
		}
		if prev.reason == teardownReasonDelete || reason == teardownReasonDelete {
			job.reason = teardownReasonDelete
		}
	}
	q.pending[namespace] = job
	depth, inflight := len(q.pending), len(q.inflight)
	q.mu.Unlock()
	q.h.setTeardownGauges(depth, inflight)
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *teardownQueue) next() (string, teardownJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for namespace, job := range q.pending {
		delete(q.pending, namespace)
		q.inflight[namespace] = struct{}{}
		q.h.setTeardownGauges(len(q.pending), len(q.inflight))
		if len(q.pending) > 0 {
			select {
			case q.wake <- struct{}{}:
			default:
			}
		}
		return namespace, job, true
	}
	return "", teardownJob{}, false
}

func (q *teardownQueue) finish(namespace string) {
	q.mu.Lock()
	delete(q.inflight, namespace)
	depth, inflight := len(q.pending), len(q.inflight)
	q.mu.Unlock()
	q.h.setTeardownGauges(depth, inflight)
}

func (q *teardownQueue) loop() {
	defer q.wg.Done()
	for {
		namespace, job, ok := q.next()
		if !ok {
			<-q.wake
			continue
		}
		if err := q.h.runTeardown(namespace, job.at, job.reason); err != nil {
			log.Printf("teardown %s: %v", namespace, err)
		}
		q.finish(namespace)
	}
}

func (h *Handler) ensureTeardownWorkers() {
	if h == nil {
		return
	}
	h.teardownOnce.Do(func() {
		h.teardown = newTeardownQueue(
			h,
			positiveIntEnv("TEARDOWN_WORKERS", 8),
			positiveIntEnv("TEARDOWN_DIAGNOSTICS_SLOTS", 8),
			durationEnv("TEARDOWN_DIAGNOSTICS_WAIT", 10*time.Second),
		)
		h.teardown.start()
	})
}

func (h *Handler) enqueueTeardown(namespace string, at time.Time, reason string) {
	h.ensureTeardownWorkers()
	h.teardown.enqueue(namespace, at, reason)
}

// RequestTeardown claims a live row and queues namespace deletion.
// The expiry timer calls this. A second call while terminating only queues.
func (h *Handler) RequestTeardown(namespace string) {
	if h == nil || namespace == "" || h.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ok, err := h.store.ClaimStatus(ctx, namespace, statusesThatMayTearDown, statusTerminating)
	if err != nil {
		log.Printf("teardown claim %s: %v", namespace, err)
		return
	}
	if !ok {
		row, getErr := h.store.GetDeployment(ctx, namespace)
		if getErr != nil || row.Status != statusTerminating {
			return
		}
	}
	h.enqueueTeardown(namespace, time.Now(), teardownReasonExpiry)
}

// StartTeardownMaintenance queues every terminating row, then repeats that
// scan so a crashed worker does not leave the row stuck.
func (h *Handler) StartTeardownMaintenance(ctx context.Context) {
	if h == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.ensureTeardownWorkers()
	h.recoverTerminating(ctx)
	go h.teardownSweep(ctx)
}

func (h *Handler) teardownSweep(ctx context.Context) {
	ticker := time.NewTicker(teardownSweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.recoverTerminating(ctx)
			h.refreshOrphanNamespaces(ctx)
		}
	}
}

func (h *Handler) recoverTerminating(ctx context.Context) {
	if h == nil || h.store == nil {
		return
	}
	rows, err := h.store.ListDeploymentsByStatus(ctx, []string{statusTerminating})
	if err != nil {
		log.Printf("teardown recover: %v", err)
		return
	}
	now := time.Now()
	for _, row := range rows {
		// The delete-request time is not a column. After a restart, lag starts
		// at expires_at when that instant is already past, otherwise at recover time.
		intent := now
		if !row.ExpiresAt.IsZero() && row.ExpiresAt.Before(now) {
			intent = row.ExpiresAt
		}
		h.enqueueTeardown(row.Namespace, intent, teardownReasonExpiry)
	}
}

func (h *Handler) runTeardown(namespace string, enqueued time.Time, reason string) error {
	ctx, cancel := context.WithTimeout(context.Background(), teardownJobBudget)
	defer cancel()

	if h.loadgen != nil {
		stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
		_ = h.loadgen.Stop(stopCtx, namespace)
		stopCancel()
	}

	if h.store == nil {
		return fmt.Errorf("api: deployment store unavailable")
	}
	row, rowErr := h.store.GetDeployment(ctx, namespace)
	if rowErr != nil {
		return fmt.Errorf("api: read deployment: %w", rowErr)
	}
	if row.Status == statusTerminated {
		return nil
	}
	if row.Status != statusTerminating {
		ok, err := h.store.ClaimStatus(ctx, namespace, statusesThatMayTearDown, statusTerminating)
		if err != nil {
			return fmt.Errorf("api: claim terminating: %w", err)
		}
		if !ok {
			return nil
		}
	}

	tpl := ""
	if rowErr == nil {
		tpl = row.Template
	}
	h.persistTeardownSummary(ctx, namespace, tpl, row, rowErr)
	h.collectTeardownDiagnostics(ctx, namespace, tpl, reason, row, rowErr)
	h.ForgetActAsCache(namespace)
	h.StopLBEndpointsWatch(namespace)
	if h.metricsCollector != nil {
		h.metricsCollector.Cleanup(namespace)
		h.refreshWebsocketLiveGauge()
	}

	var d deployer.ComponentDeployer
	if h.deployers != nil {
		d = h.deployers["db"]
	}
	if d == nil {
		return fmt.Errorf("api: teardown missing db deployer")
	}

	expires := time.Time{}
	if rowErr == nil {
		expires = row.ExpiresAt
	}
	issued := time.Now()
	h.ObserveTeardownLag(teardownIntent(enqueued, expires))
	if err := d.Teardown(ctx, namespace); err != nil {
		return fmt.Errorf("api: teardown namespace: %w", err)
	}
	if err := h.waitNamespaceGone(ctx, namespace); err != nil {
		return err
	}
	h.observeTeardownComplete(time.Since(issued))

	if h.store != nil {
		ok, err := h.store.ClaimStatus(ctx, namespace, []string{statusTerminating}, statusTerminated)
		if err != nil {
			return fmt.Errorf("api: mark terminated: %w", err)
		}
		if !ok {
			fresh, getErr := h.store.GetDeployment(ctx, namespace)
			if getErr != nil {
				return fmt.Errorf("api: read status after teardown: %w", getErr)
			}
			if fresh.Status != statusTerminated {
				return fmt.Errorf("api: teardown lost status claim, status %s", fresh.Status)
			}
		}
	}
	h.SchedulePostmortemAfterTeardown(namespace)
	h.wakeDeployQueue()
	return nil
}

func (h *Handler) persistTeardownSummary(ctx context.Context, namespace, tpl string, row store.DeploymentRow, rowErr error) {
	if h.metricsCollector == nil || h.store == nil {
		return
	}
	collectCtx, cancel := context.WithTimeout(ctx, teardownCollectBudget)
	defer cancel()
	design, _ := deployer.DesignFromMap(row.Design)
	if design == nil {
		design = &deployer.DeploymentDesign{}
	}
	topo := MetricsTopologyFromDeploymentDesign(design, tpl)
	dm, err := h.metricsCollector.Collect(collectCtx, namespace, tpl, topo)
	if err != nil || dm == nil {
		return
	}
	var base map[string]any
	if rowErr == nil {
		base = row.MetricsSummary
	}
	hist := h.metricsCollector.DownsampledSessionHistory(namespace)
	summary := h.metricsCollector.SessionSummary(namespace, base, dm)
	if rowErr == nil {
		h.EnrichTeardownSummary(collectCtx, namespace, row, summary, hist, dm)
		summary["actual_duration_seconds"] = int(time.Since(row.CreatedAt).Seconds())
	}
	summary["terminated_at"] = time.Now().Format(time.RFC3339)
	if err := h.store.SetMetricsSummary(context.Background(), namespace, summary); err != nil {
		log.Printf("teardown summary %s: %v", namespace, err)
	}
}

func (h *Handler) collectTeardownDiagnostics(ctx context.Context, namespace, tpl, reason string, row store.DeploymentRow, rowErr error) {
	if h.diagnostics == nil || h.teardown == nil {
		return
	}
	if !acquireDiagnosticsSlot(h.teardown.diagSem, h.teardown.diagWait) {
		h.incTeardownDiagnostics("skipped")
		h.stampMetrics(context.Background(), namespace, map[string]any{
			"diagnostics_skipped": true,
		})
		return
	}
	defer releaseDiagnosticsSlot(h.teardown.diagSem)

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
	if reason == "" {
		reason = teardownReasonExpiry
	}
	diagCtx, cancel := context.WithTimeout(ctx, teardownDiagCollectBudget)
	defer cancel()
	url, signatures, partial, diagErr := h.diagnostics.Collect(diagCtx, diagnostics.CollectParams{
		Namespace:        namespace,
		Template:         tpl,
		Outcome:          "success",
		CollectionReason: reason,
		CreatedAt:        createdAt,
		RunningAt:        runningAt,
		DurationSeconds:  durationSeconds,
	})
	if diagErr != nil {
		h.incTeardownDiagnostics("error")
		log.Printf("DDC: teardown collection failed for %s: %v", namespace, diagErr)
	} else {
		h.incTeardownDiagnostics("collected")
	}
	h.writeDiagnosticsSummary(namespace, url, "success", reason, signatures, partial, diagErr)
}

func (h *Handler) waitNamespaceGone(ctx context.Context, namespace string) error {
	if h.clientset == nil {
		return nil
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		_, err := h.clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil && ctx.Err() == nil {
			log.Printf("teardown %s: namespace get: %v", namespace, err)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("api: wait for namespace delete: %w", ctx.Err())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("api: wait for namespace delete: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// refreshOrphanNamespaces counts cloud9-* namespaces older than 5 minutes
// that are not provisioning, running, queued, or terminating.
func (h *Handler) refreshOrphanNamespaces(ctx context.Context) {
	if h == nil || h.clientset == nil || h.store == nil || h.cpMetrics == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	list, err := h.clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("orphan namespaces: %v", err)
		return
	}
	active, err := h.store.ListActiveDeployments(ctx, "")
	if err != nil {
		log.Printf("orphan namespaces: %v", err)
		return
	}
	live := make(map[string]struct{}, len(active))
	for _, row := range active {
		live[row.Namespace] = struct{}{}
	}
	cutoff := time.Now().Add(-orphanNamespaceMinAge)
	n := 0
	for i := range list.Items {
		ns := list.Items[i]
		name := ns.Name
		if name == "cloud9-system" || !strings.HasPrefix(name, "cloud9-") {
			continue
		}
		if ns.CreationTimestamp.Time.After(cutoff) {
			continue
		}
		if _, ok := live[name]; ok {
			continue
		}
		n++
	}
	h.setOrphanNamespaces(n)
}

// teardownIntent is the clock start for cloud9_teardown_lag_seconds.
// expires wins when it is earlier than the enqueue time.
func teardownIntent(enqueued, expires time.Time) time.Time {
	if !expires.IsZero() && (enqueued.IsZero() || expires.Before(enqueued)) {
		return expires
	}
	if enqueued.IsZero() {
		return time.Now()
	}
	return enqueued
}

func acquireDiagnosticsSlot(sem chan struct{}, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case sem <- struct{}{}:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case sem <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

func releaseDiagnosticsSlot(sem chan struct{}) {
	select {
	case <-sem:
	default:
	}
}

func positiveIntEnv(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return def
	}
	return n
}

func durationEnv(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return def
	}
	return d
}
