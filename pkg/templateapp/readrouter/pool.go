package readrouter

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// QueryRead executes a single-row read on the routed pool.
// build receives readTokenExpr when routed to replica; include it as the last
// selected column (text). scan must read application columns and, when
// replayToken is non-nil, scan the replay token into *replayToken last.
// The returned target is the hostname used for postgres.Observe (postgres on leader).
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

	route := r.RouteRead(clientID, r.policy)
	if route == RouteReplica && len(r.readPools) > 0 {
		target, err := r.queryReplicaRoundRobin(ctx, clientID, build, scan)
		if err == nil || errors.Is(err, pgx.ErrNoRows) {
			return RouteReplica, target, err
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
	dbReadTotal.WithLabelValues(RouteLeader.String()).Inc()
	return r.leaderTarget(), nil
}

func (r *ReadRouter) queryReplicaRoundRobin(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
) (string, error) {
	tokenExpr := r.engine.ReadTokenExpr()
	start := r.nextReplicaStart()
	var lastErr error

	for i := 0; i < len(r.readPools); i++ {
		idx := (start + i) % len(r.readPools)
		entry := r.readPools[idx]
		var tokenText string
		replayToken := &tokenText
		sql, args := build(tokenExpr)
		row := entry.pool.QueryRow(ctx, sql, args...)
		err := scan(row, replayToken)
		if err == nil {
			dbReadTotal.WithLabelValues(RouteReplica.String()).Inc()
			if tokenText != "" {
				tok, parseErr := r.engine.ParseToken(tokenText)
				if parseErr != nil {
					return entry.host, fmt.Errorf("parse replay token: %w", parseErr)
				}
				r.RecordRead(clientID, tok)
			}
			return entry.host, nil
		}
		if errors.Is(err, pgx.ErrNoRows) {
			dbReadTotal.WithLabelValues(RouteReplica.String()).Inc()
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
