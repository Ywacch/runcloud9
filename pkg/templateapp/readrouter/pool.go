package readrouter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// QueryRead executes a single-row read on the routed pool.
// build receives readTokenExpr when routed to replica; include it as the last
// selected column (text). scan must read application columns and, when
// replayToken is non-nil, scan the replay token into *replayToken last.
func (r *ReadRouter) QueryRead(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
) (Route, error) {
	if r == nil {
		return RouteLeader, fmt.Errorf("readrouter: nil ReadRouter")
	}
	if r.writePool == nil {
		return RouteLeader, fmt.Errorf("readrouter: nil write pool")
	}

	route := r.RouteRead(clientID, r.policy)
	pool := r.writePool
	var replayToken *string
	tokenExpr := ""
	if route == RouteReplica {
		if r.readPool == nil {
			route = RouteLeader
		} else {
			pool = r.readPool
			tokenExpr = r.engine.ReadTokenExpr()
			var tokenText string
			replayToken = &tokenText
		}
	}

	sql, args := build(tokenExpr)
	row := pool.QueryRow(ctx, sql, args...)
	if err := scan(row, replayToken); err != nil {
		return route, err
	}

	dbReadTotal.WithLabelValues(route.String()).Inc()

	if replayToken != nil && *replayToken != "" {
		tok, err := r.engine.ParseToken(*replayToken)
		if err != nil {
			return route, fmt.Errorf("parse replay token: %w", err)
		}
		r.RecordRead(clientID, tok)
	}
	return route, nil
}
