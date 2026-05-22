package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleFollow_rejectsNonPost(t *testing.T) {
	t.Parallel()
	s := &Server{db: newDB(nil)}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/follow", nil)
	s.handleFollow(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: want 405, got %d", rec.Code)
	}
}

func TestHandleFollow_rejectsInvalidIDs(t *testing.T) {
	t.Parallel()
	s := &Server{db: newDB(nil)}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/follow", strings.NewReader(`{"follower_id":0,"following_id":1}`))
	s.handleFollow(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleUnfollow_rejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	s := &Server{db: newDB(nil)}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/unfollow", strings.NewReader("not-json"))
	s.handleUnfollow(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d", rec.Code)
	}
}

func TestHandleUsersList_rejectsNonGet(t *testing.T) {
	t.Parallel()
	s := &Server{db: newDB(nil)}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/users", nil)
	s.handleUsersList(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: want 405, got %d", rec.Code)
	}
}
