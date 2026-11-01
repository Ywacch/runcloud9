package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeSocialStore struct {
	pingErr error
	pingN   int
}

func (f *fakeSocialStore) InitSchema(context.Context) error  { return nil }
func (f *fakeSocialStore) SeedIfEmpty(context.Context) error { return nil }
func (f *fakeSocialStore) Ping(context.Context) error        { f.pingN++; return f.pingErr }
func (f *fakeSocialStore) CreateUser(context.Context, string) (User, error) {
	return User{}, errors.New("not implemented")
}
func (f *fakeSocialStore) GetUser(context.Context, int) (User, error) { return User{}, ErrNotFound }
func (f *fakeSocialStore) SearchUsersByUsername(context.Context, string, int) ([]User, error) {
	return nil, nil
}
func (f *fakeSocialStore) ListUsers(context.Context, int) ([]User, error) { return nil, nil }
func (f *fakeSocialStore) InsertPost(context.Context, int, string, int) (Post, error) {
	return Post{}, errors.New("not implemented")
}
func (f *fakeSocialStore) GetFeedPull(context.Context, int, int) ([]FeedItem, error) { return nil, nil }
func (f *fakeSocialStore) GetThread(context.Context, int) (Thread, error) {
	return Thread{}, errors.New("not implemented")
}
func (f *fakeSocialStore) Follow(context.Context, int, int) error               { return nil }
func (f *fakeSocialStore) Unfollow(context.Context, int, int) error             { return nil }
func (f *fakeSocialStore) GetFollowers(context.Context, int) ([]int, error)     { return nil, nil }
func (f *fakeSocialStore) QueryUserPool(context.Context, string) ([]int, error) { return nil, nil }
func (f *fakeSocialStore) ListThreadPool(context.Context, int) ([]ThreadPoolEntry, error) {
	return nil, nil
}

func TestHandleHealth_usesSocialStorePing(t *testing.T) {
	t.Parallel()
	store := &fakeSocialStore{}
	s := &Server{store: store}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	s.handleHealth(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if store.pingN != 1 {
		t.Fatalf("Ping calls: got %d want 1", store.pingN)
	}
}

func TestHandleHealth_dbPingFailure(t *testing.T) {
	t.Parallel()
	store := &fakeSocialStore{pingErr: errors.New("down")}
	s := &Server{store: store}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	s.handleHealth(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: want 500, got %d", rec.Code)
	}
}
