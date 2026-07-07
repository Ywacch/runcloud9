package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type userRow struct {
	IsInternal           bool       `json:"is_internal"`
	IsPremium            bool       `json:"is_premium"`
	PremiumExpiresAt     *time.Time `json:"premium_expires_at"`
	SubscriptionTier     *string    `json:"subscription_tier"`
	SubscriptionStatus   *string    `json:"subscription_status"`
}

// GetUser loads billing and admin fields for a Supabase auth user ID.
func (s *DeploymentStore) GetUser(ctx context.Context, userID string) (UserRecord, error) {
	if s == nil || s.client == nil {
		return UserRecord{}, fmt.Errorf("store.GetUser: store unavailable")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return UserRecord{}, fmt.Errorf("store.GetUser: %w", ErrUserNotFound)
	}

	data, _, err := s.client.From("users").
		Select("is_internal,is_premium,premium_expires_at,subscription_tier,subscription_status", "exact", false).
		Eq("id", userID).
		Limit(1, "").
		Single().
		Execute()
	if err != nil {
		if isPostgrestNoRows(err) {
			return UserRecord{}, fmt.Errorf("store.GetUser: %w", ErrUserNotFound)
		}
		return UserRecord{}, fmt.Errorf("store.GetUser: %w", err)
	}

	var row userRow
	if err := json.Unmarshal(data, &row); err != nil {
		return UserRecord{}, fmt.Errorf("store.GetUser: parse: %w", err)
	}

	return userRowToRecord(row), nil
}

func userRowToRecord(row userRow) UserRecord {
	rec := UserRecord{
		IsInternal: row.IsInternal,
		IsPremium:  row.IsPremium,
	}
	if row.PremiumExpiresAt != nil {
		t := row.PremiumExpiresAt.UTC()
		rec.PremiumExpiresAt = &t
	}
	if row.SubscriptionTier != nil {
		rec.SubscriptionTier = *row.SubscriptionTier
	}
	if row.SubscriptionStatus != nil {
		rec.SubscriptionStatus = *row.SubscriptionStatus
	}
	return rec
}

func isPostgrestNoRows(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "0 rows") ||
		strings.Contains(msg, "json object requested") ||
		errors.Is(err, ErrUserNotFound)
}
