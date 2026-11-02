package storetest

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/ywacch/cloud9/images/url-shortener/urlstore"
)

// Fake is an in-memory urlstore.Store for conformance tests.
type Fake struct {
	mu    sync.Mutex
	rows  map[string]urlstore.Record
	pingN int
}

// NewFake returns an empty fake store.
func NewFake() *Fake {
	return &Fake{rows: make(map[string]urlstore.Record)}
}

func (f *Fake) Ping(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingN++
	return nil
}

func (f *Fake) InitSchema(context.Context) error { return nil }

func (f *Fake) Close() {}

func (f *Fake) InsertURL(_ context.Context, code, original string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[code]; ok {
		return false, nil
	}
	f.rows[code] = urlstore.Record{
		Code:      code,
		Original:  original,
		CreatedAt: time.Now().UTC(),
	}
	return true, nil
}

func (f *Fake) GetURL(_ context.Context, code string) (urlstore.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.rows[code]
	if !ok {
		return urlstore.Record{}, pgx.ErrNoRows
	}
	return rec, nil
}

func (f *Fake) URLExists(_ context.Context, code string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.rows[code]
	return ok, nil
}

func (f *Fake) ListCodes(_ context.Context, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 200
	}
	codes := make([]string, 0, len(f.rows))
	for code := range f.rows {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	if len(codes) > limit {
		codes = codes[:limit]
	}
	return codes, nil
}

var _ urlstore.Store = (*Fake)(nil)
