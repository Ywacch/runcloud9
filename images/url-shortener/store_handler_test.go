package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/images/url-shortener/storetest"
)

func TestHandleHealth_usesStorePing(t *testing.T) {
	t.Parallel()
	fake := storetest.NewFake()
	s := &Server{store: fake, cfg: Config{CacheMode: "redis"}}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	s.handleHealth(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleHealth_storePingFailure(t *testing.T) {
	t.Parallel()
	s := &Server{
		store: &pingFailStore{},
		cfg:   Config{CacheMode: "redis"},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	s.handleHealth(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: want 500, got %d", rec.Code)
	}
}

type pingFailStore struct {
	storetest.Fake
}

func (pingFailStore) Ping(context.Context) error {
	return errors.New("down")
}
