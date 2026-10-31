package main

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
)

const timelineReadClientID = "timeline"

type timelineRouterReader struct {
	router readSplitRouter
}

func newTimelineRouterReader(router readSplitRouter) timeline.PGReader {
	if router == nil {
		return nil
	}
	return timelineRouterReader{router: router}
}

func (r timelineRouterReader) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	var rows pgx.Rows
	var queryErr error
	_, _, err := r.router.QueryRows(ctx, timelineReadClientID, func(ctx context.Context, q readrouter.Querier) error {
		rows, queryErr = q.Query(ctx, sql, args...)
		return queryErr
	})
	if err != nil {
		return nil, err
	}
	return rows, queryErr
}
