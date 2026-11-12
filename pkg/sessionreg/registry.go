package sessionreg

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

var errDeploymentNotFound = fmt.Errorf("store.GetDeployment: not found")

// Registry is a write-through cache of active session rows keyed by namespace.
type Registry struct {
	*store.DeploymentStore

	mu       sync.RWMutex
	active   map[string]store.DeploymentRow
	inactive map[string]struct{}
	now      func() time.Time
	obs      Observer
	wg       sync.WaitGroup

	readOverride  readOverride
	writeOverride writeOverride
}

type readOverride interface {
	GetDeployment(ctx context.Context, namespace string) (store.DeploymentRow, error)
	ListActiveForRegistry(ctx context.Context) ([]store.DeploymentRow, error)
}

type writeOverride interface {
	CreateDeployment(ctx context.Context, params store.CreateDeploymentParams) (string, error)
	UpdateStatus(ctx context.Context, namespace, status string) error
}

// New builds a registry decorator around the Supabase store.
func New(inner *store.DeploymentStore, now func() time.Time) *Registry {
	if now == nil {
		now = time.Now
	}
	return &Registry{
		DeploymentStore: inner,
		active:          make(map[string]store.DeploymentRow),
		inactive:        make(map[string]struct{}),
		now:             now,
	}
}

// SetObserver attaches Prometheus hooks. Nil is allowed.
func (r *Registry) SetObserver(obs Observer) {
	if r == nil {
		return
	}
	r.obs = obs
}

// Load replaces the active map from one Supabase read.
func (r *Registry) Load(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("sessionreg.Load: registry is nil")
	}
	start := time.Now()
	rows, err := r.listActive(ctx)
	dur := time.Since(start).Seconds()
	if r.obs != nil {
		r.obs.ObserveSessionRegistryResync(dur)
	}
	if err != nil {
		return err
	}
	next := make(map[string]store.DeploymentRow, len(rows))
	for _, row := range rows {
		next[row.Namespace] = row
	}
	r.mu.Lock()
	r.active = next
	for ns := range next {
		delete(r.inactive, ns)
	}
	r.mu.Unlock()
	r.setRowsGauge(len(next))
	return nil
}

// Start runs Load immediately, then every 60s until ctx is cancelled.
func (r *Registry) Start(ctx context.Context) {
	if r == nil {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		_ = r.Load(ctx)
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = r.Load(ctx)
			}
		}
	}()
}

// GetDeployment returns a cached active row or falls back to the inner store.
func (r *Registry) GetDeployment(ctx context.Context, namespace string) (store.DeploymentRow, error) {
	if r == nil || r.DeploymentStore == nil {
		return store.DeploymentRow{}, fmt.Errorf("sessionreg.GetDeployment: registry is nil")
	}
	if row, ok := r.activeRow(namespace); ok {
		return row, nil
	}
	if r.isInactive(namespace) {
		if r.obs != nil {
			r.obs.IncSessionRegistryFallback("inactive")
		}
		return store.DeploymentRow{}, errDeploymentNotFound
	}
	row, err := r.innerGetDeployment(ctx, namespace)
	if err != nil {
		if r.obs != nil {
			if isNotFound(err) {
				r.obs.IncSessionRegistryFallback("fetched")
			} else {
				r.obs.IncSessionRegistryFallback("error")
			}
		}
		return store.DeploymentRow{}, err
	}
	if r.obs != nil {
		r.obs.IncSessionRegistryFallback("fetched")
	}
	if isTerminalStatus(row.Status) {
		r.markInactive(namespace)
		return store.DeploymentRow{}, errDeploymentNotFound
	}
	if isActiveStatus(row.Status) {
		r.putActive(namespace, row)
	}
	return row, nil
}

// AdmissionOccupancy sums admitted weights for provisioning, running, and terminating rows.
// Queued rows are excluded. The read holds the registry lock.
func (r *Registry) AdmissionOccupancy() store.AdmissionOccupancy {
	if r == nil {
		return store.AdmissionOccupancy{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var occ store.AdmissionOccupancy
	for _, row := range r.active {
		switch row.Status {
		case "provisioning", "running", "terminating":
			if row.AdmittedCPUm != nil {
				occ.CPUMillicores += *row.AdmittedCPUm
			}
			if row.AdmittedMemMi != nil {
				occ.MemoryMi += *row.AdmittedMemMi
			}
		}
	}
	return occ
}

// ListActiveDeployments returns cached active rows without calling Supabase.
func (r *Registry) ListActiveDeployments(ctx context.Context, userID string) ([]store.ActiveDeploymentRow, error) {
	if r == nil {
		return nil, fmt.Errorf("sessionreg.ListActiveDeployments: registry is nil")
	}
	_ = ctx
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]store.ActiveDeploymentRow, 0, len(r.active))
	userID = strings.TrimSpace(userID)
	for _, row := range r.active {
		if userID != "" && row.UserID != userID {
			continue
		}
		out = append(out, activeRowFromDeployment(row))
	}
	return out, nil
}

// CreateDeployment inserts via the inner store and seeds the cache when active.
func (r *Registry) CreateDeployment(ctx context.Context, params store.CreateDeploymentParams) (string, error) {
	id, err := r.createDeployment(ctx, params)
	if err != nil {
		return "", err
	}
	status := params.Status
	if status == "" {
		status = "provisioning"
	}
	if !isActiveStatus(status) {
		r.setRowsGauge(r.activeCount())
		return id, nil
	}
	row := deploymentRowFromCreate(params, id, status, r.now())
	r.putActive(params.Namespace, row)
	return id, nil
}

// UpdateStatus updates the inner store then patches or evicts the cache entry.
func (r *Registry) UpdateStatus(ctx context.Context, namespace, status string) error {
	if err := r.updateStatus(ctx, namespace, status); err != nil {
		return err
	}
	r.applyStatusChange(namespace, status)
	return nil
}

// ClaimStatus updates memory only when the claim succeeds.
func (r *Registry) ClaimStatus(ctx context.Context, namespace string, from []string, to string) (bool, error) {
	ok, err := r.DeploymentStore.ClaimStatus(ctx, namespace, from, to)
	if err != nil || !ok {
		return ok, err
	}
	r.applyStatusChange(namespace, to)
	return true, nil
}

// ClaimQueued promotes a queued row and sets status to provisioning in memory when present.
func (r *Registry) ClaimQueued(ctx context.Context, namespace string) (bool, error) {
	ok, err := r.DeploymentStore.ClaimQueued(ctx, namespace)
	if err != nil || !ok {
		return ok, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	row, exists := r.active[namespace]
	if !exists {
		r.setRowsGaugeLocked(len(r.active))
		return true, nil
	}
	row.Status = "provisioning"
	r.active[namespace] = row
	delete(r.inactive, namespace)
	r.setRowsGaugeLocked(len(r.active))
	return true, nil
}

// UpdateExpiresAt patches expires_at in the cache when the row is present.
func (r *Registry) UpdateExpiresAt(ctx context.Context, namespace string, expiresAt time.Time) error {
	if err := r.DeploymentStore.UpdateExpiresAt(ctx, namespace, expiresAt); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.active[namespace]
	if !ok {
		r.setRowsGaugeLocked(len(r.active))
		return nil
	}
	row.ExpiresAt = expiresAt
	r.active[namespace] = row
	r.setRowsGaugeLocked(len(r.active))
	return nil
}

// SetMetricsSummary writes through and replaces metrics_summary when cached.
func (r *Registry) SetMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error {
	if err := r.DeploymentStore.SetMetricsSummary(ctx, namespace, summary); err != nil {
		return err
	}
	r.patchMetricsSummary(namespace, summary)
	return nil
}

// UpdateMetricsSummary writes through and replaces metrics_summary when cached.
func (r *Registry) UpdateMetricsSummary(ctx context.Context, namespace string, summary map[string]any) error {
	if err := r.DeploymentStore.UpdateMetricsSummary(ctx, namespace, summary); err != nil {
		return err
	}
	r.patchMetricsSummary(namespace, summary)
	return nil
}

func (r *Registry) listActive(ctx context.Context) ([]store.DeploymentRow, error) {
	if r.readOverride != nil {
		return r.readOverride.ListActiveForRegistry(ctx)
	}
	return r.DeploymentStore.ListActiveForRegistry(ctx)
}

func (r *Registry) innerGetDeployment(ctx context.Context, namespace string) (store.DeploymentRow, error) {
	if r.readOverride != nil {
		return r.readOverride.GetDeployment(ctx, namespace)
	}
	return r.DeploymentStore.GetDeployment(ctx, namespace)
}

func (r *Registry) createDeployment(ctx context.Context, params store.CreateDeploymentParams) (string, error) {
	if r.writeOverride != nil {
		return r.writeOverride.CreateDeployment(ctx, params)
	}
	return r.DeploymentStore.CreateDeployment(ctx, params)
}

func (r *Registry) updateStatus(ctx context.Context, namespace, status string) error {
	if r.writeOverride != nil {
		return r.writeOverride.UpdateStatus(ctx, namespace, status)
	}
	return r.DeploymentStore.UpdateStatus(ctx, namespace, status)
}

func (r *Registry) activeRow(namespace string) (store.DeploymentRow, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.active[namespace]
	if !ok {
		return store.DeploymentRow{}, false
	}
	return row, true
}

func (r *Registry) isInactive(namespace string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.inactive[namespace]
	return ok
}

func (r *Registry) markInactive(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, namespace)
	r.inactive[namespace] = struct{}{}
	r.setRowsGaugeLocked(len(r.active))
}

func (r *Registry) putActive(namespace string, row store.DeploymentRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[namespace] = row
	delete(r.inactive, namespace)
	r.setRowsGaugeLocked(len(r.active))
}

func (r *Registry) applyStatusChange(namespace, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if isActiveStatus(status) {
		if row, ok := r.active[namespace]; ok {
			row.Status = status
			r.active[namespace] = row
			delete(r.inactive, namespace)
		}
		r.setRowsGaugeLocked(len(r.active))
		return
	}
	delete(r.active, namespace)
	r.inactive[namespace] = struct{}{}
	r.setRowsGaugeLocked(len(r.active))
}

func (r *Registry) patchMetricsSummary(namespace string, summary map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.active[namespace]
	if !ok {
		r.setRowsGaugeLocked(len(r.active))
		return
	}
	if summary == nil {
		summary = map[string]any{}
	}
	row.MetricsSummary = summary
	r.active[namespace] = row
	r.setRowsGaugeLocked(len(r.active))
}

func (r *Registry) activeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.active)
}

func (r *Registry) setRowsGauge(n int) {
	if r.obs != nil {
		r.obs.SetSessionRegistryRows(n)
	}
}

func (r *Registry) setRowsGaugeLocked(n int) {
	if r.obs != nil {
		r.obs.SetSessionRegistryRows(n)
	}
}

func isActiveStatus(status string) bool {
	switch status {
	case "running", "provisioning", "queued", "terminating":
		return true
	default:
		return false
	}
}

func isTerminalStatus(status string) bool {
	return status == "terminated" || status == "failed"
}

func isNotFound(err error) bool {
	return err != nil && err.Error() == errDeploymentNotFound.Error()
}

func activeRowFromDeployment(row store.DeploymentRow) store.ActiveDeploymentRow {
	return store.ActiveDeploymentRow{
		ID:        row.ID,
		Namespace: row.Namespace,
		Template:  row.Template,
		Status:    row.Status,
		ExpiresAt: row.ExpiresAt,
		CreatedAt: row.CreatedAt,
	}
}

func deploymentRowFromCreate(params store.CreateDeploymentParams, id, status string, now time.Time) store.DeploymentRow {
	var comparisonID, runRole *string
	if params.ComparisonID != "" {
		comparisonID = &params.ComparisonID
	}
	if params.RunRole != "" {
		runRole = &params.RunRole
	}
	cpu := params.AdmittedCPUm
	mem := params.AdmittedMemMi
	return store.DeploymentRow{
		ID:            id,
		UserID:        params.UserID,
		Namespace:     params.Namespace,
		Template:      params.Template,
		Archetype:     params.Archetype,
		Status:        status,
		Design:        params.Design,
		ExpiresAt:     params.ExpiresAt,
		CreatedAt:     now,
		ComparisonID:  comparisonID,
		RunRole:       runRole,
		AdmittedCPUm:  &cpu,
		AdmittedMemMi: &mem,
	}
}
