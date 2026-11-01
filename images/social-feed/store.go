package main

import "context"

// SocialStore is the social-feed main_db job. Postgres is today's only implementation.
type SocialStore interface {
	InitSchema(ctx context.Context) error
	SeedIfEmpty(ctx context.Context) error
	Ping(ctx context.Context) error
	CreateUser(ctx context.Context, username string) (User, error)
	GetUser(ctx context.Context, id int) (User, error)
	SearchUsersByUsername(ctx context.Context, query string, limit int) ([]User, error)
	ListUsers(ctx context.Context, limit int) ([]User, error)
	InsertPost(ctx context.Context, userID int, content string, replyToID int) (Post, error)
	GetFeedPull(ctx context.Context, userID, limit int) ([]FeedItem, error)
	GetThread(ctx context.Context, rootID int) (Thread, error)
	Follow(ctx context.Context, followerID, followingID int) error
	Unfollow(ctx context.Context, followerID, followingID int) error
	GetFollowers(ctx context.Context, userID int) ([]int, error)
	QueryUserPool(ctx context.Context, sql string) ([]int, error)
	ListThreadPool(ctx context.Context, limit int) ([]ThreadPoolEntry, error)
}

var _ SocialStore = (*DB)(nil)
