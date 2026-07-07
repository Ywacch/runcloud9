package api

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ywacch/cloud9/pkg/store"
)

// loadUserRecord returns the Supabase users row for the authenticated caller.
// When the store is unavailable or the user row is missing in non-production, a zero record (free tier) is returned.
func (h *Handler) loadUserRecord(ctx context.Context, userID string) (store.UserRecord, error) {
	if h.store == nil {
		return store.UserRecord{}, nil
	}
	user, err := h.store.GetUser(ctx, userID)
	if err == nil {
		return user, nil
	}
	if errors.Is(err, store.ErrUserNotFound) && os.Getenv("ENV") != "production" {
		return store.UserRecord{}, nil
	}
	return store.UserRecord{}, fmt.Errorf("load user: %w", err)
}
