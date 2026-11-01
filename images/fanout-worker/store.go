package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// FollowerStore loads author and follower ids for timeline fan-out.
type FollowerStore interface {
	GetAuthor(ctx context.Context, userID int) (username string, err error)
	GetFollowers(ctx context.Context, userID int) ([]int, error)
}

type pgFollowerStore struct {
	pool *pgxpool.Pool
}

func newPGFollowerStore(pool *pgxpool.Pool) *pgFollowerStore {
	return &pgFollowerStore{pool: pool}
}

func (s *pgFollowerStore) GetAuthor(ctx context.Context, userID int) (string, error) {
	var username string
	if err := s.pool.QueryRow(ctx, `SELECT username FROM users WHERE id = $1`, userID).Scan(&username); err != nil {
		return "", fmt.Errorf("db user lookup: %w", err)
	}
	return username, nil
}

func (s *pgFollowerStore) GetFollowers(ctx context.Context, userID int) ([]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT follower_id FROM follows WHERE following_id = $1`, userID)
	if err != nil {
		return nil, fmt.Errorf("db follower lookup: %w", err)
	}
	defer rows.Close()

	var followers []int
	for rows.Next() {
		var followerID int
		if err := rows.Scan(&followerID); err != nil {
			return nil, fmt.Errorf("db follower scan: %w", err)
		}
		followers = append(followers, followerID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db follower rows: %w", err)
	}
	return followers, nil
}

var _ FollowerStore = (*pgFollowerStore)(nil)
