package storetest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/images/social-feed/socialstore"
)

// Fake is an in-memory socialstore.Store for conformance tests.
type Fake struct {
	mu       sync.Mutex
	nextUser int
	nextPost int
	users    map[int]socialstore.User
	byName   map[string]int
	posts    map[int]socialstore.Post
	follows  map[[2]int]struct{}
	servedBy [2]string
}

// NewFake returns an empty fake store with deterministic servedBy labels.
func NewFake() *Fake {
	return &Fake{
		nextUser: 1,
		nextPost: 1,
		users:    make(map[int]socialstore.User),
		byName:   make(map[string]int),
		posts:    make(map[int]socialstore.Post),
		follows:  make(map[[2]int]struct{}),
		servedBy: [2]string{"fake-replies", "fake-parents"},
	}
}

func (f *Fake) InitSchema(context.Context) error  { return nil }
func (f *Fake) SeedIfEmpty(context.Context) error { return nil }
func (f *Fake) Ping(context.Context) error        { return nil }
func (f *Fake) QueryUserPool(context.Context, string) ([]int, error) {
	return nil, nil
}
func (f *Fake) ListThreadPool(context.Context, int) ([]socialstore.ThreadPoolEntry, error) {
	return nil, nil
}

// Reconnect is a no-op; in-memory data survives.
func (f *Fake) Reconnect(context.Context) error { return nil }

func (f *Fake) CreateUser(_ context.Context, username string) (socialstore.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.byName[username]; ok {
		return socialstore.User{}, socialstore.ErrUsernameTaken
	}
	id := f.nextUser
	f.nextUser++
	u := socialstore.User{ID: id, Username: username}
	f.users[id] = u
	f.byName[username] = id
	return u, nil
}

func (f *Fake) GetUser(_ context.Context, id int) (socialstore.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.users[id]
	if !ok {
		return socialstore.User{}, socialstore.ErrNotFound
	}
	return u, nil
}

func (f *Fake) SearchUsersByUsername(_ context.Context, query string, limit int) ([]socialstore.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 20
	}
	q := strings.ToLower(query)
	var out []socialstore.User
	for _, u := range f.users {
		if strings.Contains(strings.ToLower(u.Username), q) {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Username < out[j].Username })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *Fake) ListUsers(_ context.Context, limit int) ([]socialstore.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 20
	}
	ids := make([]int, 0, len(f.users))
	for id := range f.users {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var out []socialstore.User
	for _, id := range ids {
		out = append(out, f.users[id])
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (f *Fake) InsertPost(_ context.Context, userID int, content string, replyToID int) (socialstore.Post, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.users[userID]; !ok {
		return socialstore.Post{}, socialstore.ErrNotFound
	}
	id := f.nextPost
	f.nextPost++
	now := time.Now().UTC()
	p := socialstore.Post{
		ID:        id,
		UserID:    userID,
		Content:   content,
		CreatedAt: now,
	}
	if replyToID <= 0 {
		root := id
		p.ThreadRootID = &root
		f.posts[id] = p
		return p, nil
	}
	parent, ok := f.posts[replyToID]
	if !ok {
		return socialstore.Post{}, socialstore.ErrNotFound
	}
	reply := replyToID
	p.ReplyToID = &reply
	threadRoot := parent.ID
	if parent.ThreadRootID != nil && *parent.ThreadRootID > 0 {
		threadRoot = *parent.ThreadRootID
	}
	p.ThreadRootID = &threadRoot
	f.posts[id] = p
	return p, nil
}

func (f *Fake) GetFeedPull(_ context.Context, userID, limit int) ([]socialstore.FeedItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 20
	}
	following := make(map[int]struct{})
	for edge := range f.follows {
		if edge[0] == userID {
			following[edge[1]] = struct{}{}
		}
	}
	var items []socialstore.FeedItem
	for _, p := range f.posts {
		if _, ok := following[p.UserID]; !ok {
			continue
		}
		u := f.users[p.UserID]
		items = append(items, socialstore.FeedItem{
			PostID:    p.ID,
			AuthorID:  p.UserID,
			Username:  u.Username,
			Content:   p.Content,
			CreatedAt: p.CreatedAt,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].PostID > items[j].PostID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (f *Fake) GetThread(_ context.Context, rootID int) (socialstore.Thread, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := socialstore.Thread{RootID: rootID}
	out.ServedBy.Replies = f.servedBy[0]
	out.ServedBy.Parents = f.servedBy[1]
	var replies []socialstore.Post
	for _, p := range f.posts {
		if p.ThreadRootID != nil && *p.ThreadRootID == rootID && p.ReplyToID != nil {
			replies = append(replies, p)
		}
	}
	sort.Slice(replies, func(i, j int) bool { return replies[i].CreatedAt.Before(replies[j].CreatedAt) })
	out.Replies = replies

	seen := make(map[int]struct{})
	for _, r := range replies {
		if r.ReplyToID == nil {
			continue
		}
		pid := *r.ReplyToID
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		if parent, ok := f.posts[pid]; ok {
			out.Parents = append(out.Parents, parent)
		}
	}
	return out, nil
}

func (f *Fake) Follow(_ context.Context, followerID, followingID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.users[followerID]; !ok {
		return fmt.Errorf("follower: %w", socialstore.ErrNotFound)
	}
	if _, ok := f.users[followingID]; !ok {
		return fmt.Errorf("following: %w", socialstore.ErrNotFound)
	}
	f.follows[[2]int{followerID, followingID}] = struct{}{}
	return nil
}

func (f *Fake) Unfollow(_ context.Context, followerID, followingID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.follows, [2]int{followerID, followingID})
	return nil
}

func (f *Fake) GetFollowers(_ context.Context, userID int) ([]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []int
	for edge := range f.follows {
		if edge[1] == userID {
			ids = append(ids, edge[0])
		}
	}
	sort.Ints(ids)
	return ids, nil
}

var _ socialstore.Store = (*Fake)(nil)
