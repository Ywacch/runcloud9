package socialstore

import (
	"errors"
	"time"
)

// ErrUsernameTaken is returned when CreateUser hits the unique username constraint.
var ErrUsernameTaken = errors.New("username taken")

// ErrNotFound is returned when a row is missing.
var ErrNotFound = errors.New("not found")

// User is a social-feed account row.
type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Bio      string `json:"bio"`
}

// Post is a feed or thread post row.
type Post struct {
	ID           int       `json:"id"`
	UserID       int       `json:"user_id"`
	Content      string    `json:"content"`
	CreatedAt    time.Time `json:"created_at"`
	ReplyToID    *int      `json:"reply_to_id,omitempty"`
	ThreadRootID *int      `json:"thread_root_id,omitempty"`
}

// Thread bundles replies and parent posts for a thread root read.
type Thread struct {
	RootID   int
	Replies  []Post
	Parents  []Post
	ServedBy struct {
		Replies string `json:"replies"`
		Parents string `json:"parents"`
	}
}

// FeedItem is one row in a pull-mode home feed.
type FeedItem struct {
	PostID    int       `json:"post_id"`
	AuthorID  int       `json:"author_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// ThreadPoolEntry is one active thread root in GET /thread-pool.
type ThreadPoolEntry struct {
	RootID     int `json:"rootId"`
	ReplyCount int `json:"replyCount"`
}
