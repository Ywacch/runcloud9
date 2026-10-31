package timeline

import (
	"context"
	"sort"
	"sync"
)

const memoryEngine = "memory"

// MemoryStore is an in-memory timeline for tests and conformance.
type MemoryStore struct {
	mu         sync.Mutex
	maxEntries int
	byUser     map[int64][]Item
}

// NewMemory returns a concurrent in-memory timeline capped at maxEntries per user.
func NewMemory(maxEntries int) *MemoryStore {
	if maxEntries < 1 {
		maxEntries = 1
	}
	return &MemoryStore{
		maxEntries: maxEntries,
		byUser:     make(map[int64][]Item),
	}
}

func (m *MemoryStore) Engine() string {
	return memoryEngine
}

// Reconnect is a no-op for the in-memory store.
func (m *MemoryStore) Reconnect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (m *MemoryStore) Append(ctx context.Context, userIDs []int64, item Item) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, uid := range userIDs {
		items := m.byUser[uid]
		items = removePostID(items, item.PostID)
		items = append(items, item)
		sortTimeline(items)
		if len(items) > m.maxEntries {
			items = items[:m.maxEntries]
		}
		m.byUser[uid] = items
	}
	return nil
}

func (m *MemoryStore) Range(ctx context.Context, userID int64, limit int) ([]Item, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.byUser[userID]
	if len(src) == 0 {
		return nil, nil
	}
	out := make([]Item, len(src))
	copy(out, src)
	sortTimeline(out)
	if len(out) > limit {
		out = out[:limit]
	}
	cloned := make([]Item, len(out))
	for i := range out {
		cloned[i] = cloneItem(out[i])
	}
	return cloned, nil
}

func (m *MemoryStore) Trim(ctx context.Context, userID int64, keep int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if keep <= 0 {
		return ErrInvalidLimit
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	items := m.byUser[userID]
	if len(items) == 0 {
		return nil
	}
	sortTimeline(items)
	if len(items) > keep {
		items = items[:keep]
	}
	m.byUser[userID] = items
	return nil
}

func removePostID(items []Item, postID int64) []Item {
	if len(items) == 0 {
		return items
	}
	out := items[:0]
	for _, it := range items {
		if it.PostID != postID {
			out = append(out, it)
		}
	}
	return out
}

func sortTimeline(items []Item) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].PostID > items[j].PostID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
}

func cloneItem(it Item) Item {
	out := it
	if len(it.Payload) > 0 {
		out.Payload = append([]byte(nil), it.Payload...)
	}
	return out
}
