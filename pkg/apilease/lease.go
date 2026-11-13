package apilease

import (
	"context"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Config describes the API leadership lease in the cluster.
type Config struct {
	Namespace  string
	Name       string
	Identity   string
	Duration   time.Duration
	RenewEvery time.Duration
	Now        func() time.Time
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// CanTake reports whether identity may own the lease at now.
// A nil lease is free. An expired renew time is free.
// The holder may renew its own lease.
func CanTake(lease *coordinationv1.Lease, identity string, now time.Time, duration time.Duration) bool {
	if identity == "" {
		return false
	}
	if lease == nil {
		return true
	}
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	if holder == "" {
		return true
	}
	if holder == identity {
		return true
	}
	if lease.Spec.RenewTime == nil {
		return true
	}
	expiresAt := lease.Spec.RenewTime.Time.Add(duration)
	return !now.Before(expiresAt)
}

// Hold blocks until identity owns the lease, then renews until ctx is cancelled.
// On cancel it releases the lease when this identity holds it.
// It returns ctx.Err() when ctx is cancelled before acquire.
// After acquire it returns nil; renewal runs until ctx is cancelled.
func Hold(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	if client == nil {
		return fmt.Errorf("apilease.Hold: kubernetes client is required")
	}
	if cfg.Namespace == "" || cfg.Name == "" || cfg.Identity == "" {
		return fmt.Errorf("apilease.Hold: namespace, name, and identity are required")
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.RenewEvery <= 0 {
		cfg.RenewEvery = 10 * time.Second
	}

	if err := acquire(ctx, client, cfg); err != nil {
		return err
	}
	go func() {
		defer release(context.Background(), client, cfg)
		renewUntil(ctx, client, cfg)
	}()
	return nil
}

func acquire(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	leases := client.CoordinationV1().Leases(cfg.Namespace)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		lease, err := leases.Get(ctx, cfg.Name, metav1.GetOptions{})
		var current *coordinationv1.Lease
		switch {
		case apierrors.IsNotFound(err):
			current = nil
		case err != nil:
			return fmt.Errorf("apilease.acquire: get lease: %w", err)
		default:
			current = lease
		}
		now := cfg.now().UTC()
		if !CanTake(current, cfg.Identity, now, cfg.Duration) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.RenewEvery):
			}
			continue
		}
		if current == nil {
			created, err := leases.Create(ctx, newLease(cfg, now), metav1.CreateOptions{})
			if err != nil {
				if apierrors.IsAlreadyExists(err) {
					continue
				}
				return fmt.Errorf("apilease.acquire: create lease: %w", err)
			}
			current = created
		} else {
			updated := current.DeepCopy()
			applyHolder(updated, cfg.Identity, now, cfg.Duration)
			got, err := leases.Update(ctx, updated, metav1.UpdateOptions{})
			if err != nil {
				if apierrors.IsConflict(err) {
					continue
				}
				return fmt.Errorf("apilease.acquire: update lease: %w", err)
			}
			current = got
		}
		if current.Spec.HolderIdentity != nil && *current.Spec.HolderIdentity == cfg.Identity {
			return nil
		}
	}
}

func renewUntil(ctx context.Context, client kubernetes.Interface, cfg Config) {
	ticker := time.NewTicker(cfg.RenewEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := renew(ctx, client, cfg); err != nil {
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}

func renew(ctx context.Context, client kubernetes.Interface, cfg Config) error {
	leases := client.CoordinationV1().Leases(cfg.Namespace)
	lease, err := leases.Get(ctx, cfg.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != cfg.Identity {
		return fmt.Errorf("apilease.renew: lost lease")
	}
	now := cfg.now().UTC()
	updated := lease.DeepCopy()
	applyHolder(updated, cfg.Identity, now, cfg.Duration)
	_, err = leases.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func release(ctx context.Context, client kubernetes.Interface, cfg Config) {
	leases := client.CoordinationV1().Leases(cfg.Namespace)
	lease, err := leases.Get(ctx, cfg.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != cfg.Identity {
		return
	}
	updated := lease.DeepCopy()
	empty := ""
	updated.Spec.HolderIdentity = &empty
	now := cfg.now().UTC()
	updated.Spec.RenewTime = &metav1.MicroTime{Time: now}
	_, _ = leases.Update(ctx, updated, metav1.UpdateOptions{})
}

func newLease(cfg Config, now time.Time) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.Name,
			Namespace: cfg.Namespace,
		},
	}
	applyHolder(lease, cfg.Identity, now, cfg.Duration)
	return lease
}

func applyHolder(lease *coordinationv1.Lease, identity string, now time.Time, duration time.Duration) {
	secs := int32(duration.Seconds())
	if secs < 1 {
		secs = 1
	}
	lease.Spec.HolderIdentity = &identity
	lease.Spec.LeaseDurationSeconds = &secs
	lease.Spec.RenewTime = &metav1.MicroTime{Time: now.UTC()}
}
