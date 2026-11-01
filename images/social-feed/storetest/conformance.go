package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ywacch/cloud9/images/social-feed/socialstore"
)

// Factory returns a fresh store and a cleanup func.
type Factory func(t *testing.T) (socialstore.Store, func())

// Reconnector is optional. Stores that implement it are tested for survival across reconnect.
type Reconnector interface {
	Reconnect(ctx context.Context) error
}

// RunConformance runs shared social-feed store tests against newStore.
func RunConformance(t *testing.T, newStore Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("create_and_get_user", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		u, err := s.CreateUser(ctx, "conform_alice")
		if err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
		if u.ID <= 0 || u.Username != "conform_alice" {
			t.Fatalf("create: %+v", u)
		}
		got, err := s.GetUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetUser: %v", err)
		}
		if got.Username != u.Username {
			t.Fatalf("get: %+v", got)
		}
	})

	t.Run("search_users_by_prefix", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		if _, err := s.CreateUser(ctx, "prefix_bob"); err != nil {
			t.Fatalf("CreateUser bob: %v", err)
		}
		if _, err := s.CreateUser(ctx, "prefix_carol"); err != nil {
			t.Fatalf("CreateUser carol: %v", err)
		}
		if _, err := s.CreateUser(ctx, "other_dan"); err != nil {
			t.Fatalf("CreateUser dan: %v", err)
		}
		got, err := s.SearchUsersByUsername(ctx, "prefix_", 10)
		if err != nil {
			t.Fatalf("SearchUsersByUsername: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len: got %d want 2", len(got))
		}
	})

	t.Run("follow_and_unfollow_idempotent", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		a, err := s.CreateUser(ctx, "follow_a")
		if err != nil {
			t.Fatalf("CreateUser a: %v", err)
		}
		b, err := s.CreateUser(ctx, "follow_b")
		if err != nil {
			t.Fatalf("CreateUser b: %v", err)
		}
		if err := s.Follow(ctx, a.ID, b.ID); err != nil {
			t.Fatalf("Follow: %v", err)
		}
		if err := s.Follow(ctx, a.ID, b.ID); err != nil {
			t.Fatalf("Follow again: %v", err)
		}
		followers, err := s.GetFollowers(ctx, b.ID)
		if err != nil {
			t.Fatalf("GetFollowers: %v", err)
		}
		if len(followers) != 1 || followers[0] != a.ID {
			t.Fatalf("followers: %v", followers)
		}
		if err := s.Unfollow(ctx, a.ID, b.ID); err != nil {
			t.Fatalf("Unfollow: %v", err)
		}
		if err := s.Unfollow(ctx, a.ID, b.ID); err != nil {
			t.Fatalf("Unfollow again: %v", err)
		}
		followers, err = s.GetFollowers(ctx, b.ID)
		if err != nil {
			t.Fatalf("GetFollowers after unfollow: %v", err)
		}
		if len(followers) != 0 {
			t.Fatalf("followers after unfollow: %v", followers)
		}
	})

	t.Run("feed_pull_newest_first_and_limit", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		reader, err := s.CreateUser(ctx, "feed_reader")
		if err != nil {
			t.Fatalf("CreateUser reader: %v", err)
		}
		author, err := s.CreateUser(ctx, "feed_author")
		if err != nil {
			t.Fatalf("CreateUser author: %v", err)
		}
		if err := s.Follow(ctx, reader.ID, author.ID); err != nil {
			t.Fatalf("Follow: %v", err)
		}
		p1, err := s.InsertPost(ctx, author.ID, "first", 0)
		if err != nil {
			t.Fatalf("InsertPost first: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
		p2, err := s.InsertPost(ctx, author.ID, "second", 0)
		if err != nil {
			t.Fatalf("InsertPost second: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
		p3, err := s.InsertPost(ctx, author.ID, "third", 0)
		if err != nil {
			t.Fatalf("InsertPost third: %v", err)
		}
		items, err := s.GetFeedPull(ctx, reader.ID, 2)
		if err != nil {
			t.Fatalf("GetFeedPull: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len: got %d want 2", len(items))
		}
		if items[0].PostID != p3.ID || items[1].PostID != p2.ID {
			t.Fatalf("order: got [%d %d] want [%d %d]", items[0].PostID, items[1].PostID, p3.ID, p2.ID)
		}
		_ = p1
	})

	t.Run("insert_post_root_and_reply", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		author, err := s.CreateUser(ctx, "post_author")
		if err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
		root, err := s.InsertPost(ctx, author.ID, "root post", 0)
		if err != nil {
			t.Fatalf("InsertPost root: %v", err)
		}
		if root.ThreadRootID == nil || *root.ThreadRootID != root.ID {
			t.Fatalf("root thread_root_id: %+v", root)
		}
		reply, err := s.InsertPost(ctx, author.ID, "reply post", root.ID)
		if err != nil {
			t.Fatalf("InsertPost reply: %v", err)
		}
		if reply.ReplyToID == nil || *reply.ReplyToID != root.ID {
			t.Fatalf("reply reply_to_id: %+v", reply)
		}
	})

	t.Run("get_thread_replies_parents_and_served_by", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		author, err := s.CreateUser(ctx, "thread_author")
		if err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
		root, err := s.InsertPost(ctx, author.ID, "thread root", 0)
		if err != nil {
			t.Fatalf("InsertPost root: %v", err)
		}
		if _, err := s.InsertPost(ctx, author.ID, "thread reply", root.ID); err != nil {
			t.Fatalf("InsertPost reply: %v", err)
		}
		thread, err := s.GetThread(ctx, root.ID)
		if err != nil {
			t.Fatalf("GetThread: %v", err)
		}
		if len(thread.Replies) != 1 {
			t.Fatalf("replies: got %d want 1", len(thread.Replies))
		}
		if len(thread.Parents) != 1 {
			t.Fatalf("parents: got %d want 1", len(thread.Parents))
		}
		if thread.ServedBy.Replies == "" {
			t.Fatal("ServedBy.Replies empty")
		}
		if thread.ServedBy.Parents == "" {
			t.Fatal("ServedBy.Parents empty")
		}
	})

	t.Run("reconnect_survival", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		rc, ok := s.(Reconnector)
		if !ok {
			t.Skip("store does not implement Reconnector")
		}
		u, err := s.CreateUser(ctx, "reconnect_user")
		if err != nil {
			t.Fatalf("CreateUser: %v", err)
		}
		if err := rc.Reconnect(ctx); err != nil {
			t.Fatalf("Reconnect: %v", err)
		}
		got, err := s.GetUser(ctx, u.ID)
		if err != nil {
			t.Fatalf("GetUser after reconnect: %v", err)
		}
		if got.Username != u.Username {
			t.Fatalf("user after reconnect: %+v", got)
		}
	})
}

// RunReadYourWrites runs the read-your-writes check when the factory returns a store
// configured with read routing and client identity on ctx.
func RunReadYourWrites(t *testing.T, newStore Factory, clientCtx func(context.Context) context.Context) {
	t.Helper()
	if clientCtx == nil {
		t.Fatal("clientCtx required")
	}
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := clientCtx(context.Background())
	u, err := s.CreateUser(ctx, "ryw_user")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	got, err := s.GetUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.Username != u.Username {
		t.Fatalf("read-your-writes: create %+v get %+v", u, got)
	}
}

// AssertNotFound checks errors.Is against ErrNotFound.
func AssertNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, socialstore.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
