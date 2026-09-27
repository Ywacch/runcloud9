package readrouter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var errRYWRedirectLeader = errors.New("readrouter: ryw redirect to leader")

// QueryRead executes a single-row read on the routed pool.
// build receives readTokenExpr when routed to replica; include it as the last
// selected column (text). scan must read application columns and, when
// replayToken is non-nil, scan the replay token into *replayToken last.
// The returned target is the hostname used for postgres.Observe (postgres on leader).
// RYW and replica-stale accounting freeze lastWrite before the replica QueryRow;
// writes that complete during scan are concurrent with this read, not causally prior.
func (r *ReadRouter) QueryRead(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
) (Route, string, error) {
	if r == nil {
		return RouteLeader, defaultWriteTarget, fmt.Errorf("readrouter: nil ReadRouter")
	}
	if r.writePool == nil {
		return RouteLeader, r.leaderTarget(), fmt.Errorf("readrouter: nil write pool")
	}

	pol := r.policy
	if len(r.readPools) > 0 {
		switch pol {
		case PolicyRYW, PolicyRYWMonotonic:
			target, err := r.queryReplicaRoundRobin(ctx, clientID, build, scan, true)
			if errors.Is(err, errRYWRedirectLeader) {
				rywLeaderRedirectsTotal.Inc()
			} else if err == nil || errors.Is(err, pgx.ErrNoRows) {
				return RouteReplica, target, err
			} else if !shouldFailover(err) {
				return RouteReplica, target, err
			}
		default:
			if r.RouteRead(clientID, pol) == RouteReplica {
				target, err := r.queryReplicaRoundRobin(ctx, clientID, build, scan, false)
				if err == nil || errors.Is(err, pgx.ErrNoRows) {
					return RouteReplica, target, err
				}
				if !shouldFailover(err) {
					return RouteReplica, target, err
				}
			}
		}
	}

	target, err := r.queryLeader(ctx, clientID, build, scan)
	return RouteLeader, target, err
}

func (r *ReadRouter) queryLeader(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
) (string, error) {
	sql, args := build("")
	row := r.writePool.QueryRow(ctx, sql, args...)
	if err := scan(row, nil); err != nil {
		return r.leaderTarget(), err
	}
	incDBRead(DBReadLeaderLabel)
	return r.leaderTarget(), nil
}

func (r *ReadRouter) queryReplicaRoundRobin(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
	rywRedirect bool,
) (string, error) {
	tokenExpr := r.engine.ReadTokenExpr()
	start := r.nextReplicaStart()
	var lastErr error

	for i := 0; i < len(r.readPools); i++ {
		idx := (start + i) % len(r.readPools)
		entry := r.readPools[idx]
		writeBaseline := r.snapshotLastWrite(clientID)
		var tokenText string
		replayToken := &tokenText
		sql, args := build(tokenExpr)
		row := entry.pool.QueryRow(ctx, sql, args...)
		err := scan(row, replayToken)
		if err == nil {
			if tokenText != "" {
				tok, parseErr := r.engine.ParseToken(tokenText)
				if parseErr != nil {
					return entry.host, fmt.Errorf("parse replay token: %w", parseErr)
				}
				if rywRedirect && tokenBehindBaseline(tok, writeBaseline) {
					return "", errRYWRedirectLeader
				}
				incDBRead(entry.host)
				r.recordReadAgainst(ctx, clientID, entry.host, "", tok, writeBaseline)
				return entry.host, nil
			}
			incDBRead(entry.host)
			return entry.host, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			if rywRedirect && writeBaseline != nil {
				return "", errRYWRedirectLeader
			}
			incDBRead(entry.host)
			return entry.host, err
		}
		if shouldFailover(err) {
			lastErr = err
			continue
		}
		return entry.host, err
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("readrouter: no replica pools configured")
}

func shouldFailover(err error) bool {
	return err != nil && !errors.Is(err, pgx.ErrNoRows)
}

// QueryRows executes a multi-row read on the routed pool.
// run receives the pool chosen for the query. Routing mirrors QueryRead:
// under RYW, try replica first with a sidecar LSN peek; redirect to leader when behind.
// RYW and replica-stale accounting freeze lastWrite before the peek; writes that
// complete during run are concurrent with this read, not causally prior.
func (r *ReadRouter) QueryRows(
	ctx context.Context,
	clientID string,
	run func(ctx context.Context, q Querier) error,
) (Route, string, error) {
	if r == nil {
		return RouteLeader, defaultWriteTarget, fmt.Errorf("readrouter: nil ReadRouter")
	}
	if r.writePool == nil {
		return RouteLeader, r.leaderTarget(), fmt.Errorf("readrouter: nil write pool")
	}

	pol := r.policy
	if len(r.readPools) > 0 {
		switch pol {
		case PolicyRYW, PolicyRYWMonotonic:
			target, err := r.queryRowsReplicaRoundRobin(ctx, clientID, run, true)
			if errors.Is(err, errRYWRedirectLeader) {
				rywLeaderRedirectsTotal.Inc()
			} else if err == nil {
				return RouteReplica, target, nil
			} else if !shouldFailover(err) {
				return RouteReplica, target, err
			}
		default:
			if r.RouteRead(clientID, pol) == RouteReplica {
				target, err := r.queryRowsReplicaRoundRobin(ctx, clientID, run, false)
				if err == nil {
					return RouteReplica, target, nil
				}
				if !shouldFailover(err) {
					return RouteReplica, target, err
				}
			}
		}
	}

	if err := run(ctx, r.writePool); err != nil {
		return RouteLeader, r.leaderTarget(), err
	}
	incDBRead(DBReadLeaderLabel)
	return RouteLeader, r.leaderTarget(), nil
}

func (r *ReadRouter) queryRowsReplicaRoundRobin(
	ctx context.Context,
	clientID string,
	run func(ctx context.Context, q Querier) error,
	rywRedirect bool,
) (string, error) {
	start := r.nextReplicaStart()
	var lastErr error

	for i := 0; i < len(r.readPools); i++ {
		idx := (start + i) % len(r.readPools)
		entry := r.readPools[idx]
		writeBaseline := r.snapshotLastWrite(clientID)

		var tokenText string
		row := entry.pool.QueryRow(ctx, "SELECT "+r.engine.ReadTokenExpr())
		err := row.Scan(&tokenText)
		if errors.Is(err, pgx.ErrNoRows) {
			lastErr = err
			continue
		}
		if err != nil {
			if shouldFailover(err) {
				lastErr = err
				continue
			}
			return entry.host, err
		}

		var tok ConsistencyToken
		if tokenText != "" {
			parsed, parseErr := r.engine.ParseToken(tokenText)
			if parseErr != nil {
				return entry.host, fmt.Errorf("parse replay token: %w", parseErr)
			}
			tok = parsed
			if rywRedirect && tokenBehindBaseline(tok, writeBaseline) {
				return "", errRYWRedirectLeader
			}
		}

		if err := run(ctx, entry.pool); err != nil {
			if shouldFailover(err) {
				lastErr = err
				continue
			}
			return entry.host, err
		}

		incDBRead(entry.host)
		if tok != nil {
			r.recordReadAgainst(ctx, clientID, entry.host, "", tok, writeBaseline)
		}
		return entry.host, nil
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("readrouter: no replica pools configured")
}
