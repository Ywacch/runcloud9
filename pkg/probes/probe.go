// Package probes implements engine-swappable consistency shadow reads for Wave 3 Phase 2.
// The observer (PR2+) dials primary and replica hosts from cloud9-api; this package is the DB client only.
package probes

import (
	"context"
)

const (
	// ProbeTableName is the logical-version table written by the observer on the primary
	// and read on standbys. Rows use the _c9_ prefix so introspect and student metrics can skip them.
	ProbeTableName = "_c9_probe"
	probeRowID     = 1
)

// probeCreateTableSQL creates the single-row version table used for RYW / monotonic checks.
const probeCreateTableSQL = `
CREATE TABLE IF NOT EXISTS _c9_probe (
	id int PRIMARY KEY DEFAULT 1,
	version bigint NOT NULL,
	written_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT _c9_probe_singleton CHECK (id = 1)
)`

// ConsistencyProbe writes a monotonic logical version on the primary and reads it back on a target host.
// Postgres is the first implementation; other engines implement the same contract in later waves.
type ConsistencyProbe interface {
	EnsureSchema(ctx context.Context) error
	WriteVersion(ctx context.Context) (int64, error)
	ReadVersion(ctx context.Context) (version int64, ok bool, err error)
	Close() error
}

// MonotonicViolation reports whether a replica read regressed below the last observed version.
func MonotonicViolation(lastSeen, current int64) bool {
	return current < lastSeen
}

// StalenessVersions returns how many logical versions the read lags behind the last write (floor 0).
func StalenessVersions(written, read int64) int64 {
	if written <= read {
		return 0
	}
	return written - read
}
