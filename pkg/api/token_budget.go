package api

import (
	"os"
	"strconv"
	"strings"
	"sync"
)

const defaultInferenceSessionTokenLimit = 50_000

// SessionTokenBudget tracks per-session LLM token spend for inference calls.
type SessionTokenBudget struct {
	mu    sync.Mutex
	limit int
	used  map[string]int
}

// NewSessionTokenBudget reads INFERENCE_SESSION_TOKEN_LIMIT when set.
func NewSessionTokenBudget() *SessionTokenBudget {
	limit := defaultInferenceSessionTokenLimit
	if raw := strings.TrimSpace(os.Getenv("INFERENCE_SESSION_TOKEN_LIMIT")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	return &SessionTokenBudget{
		limit: limit,
		used:  make(map[string]int),
	}
}

// Remaining returns tokens left for sessionID (0 when exhausted).
func (b *SessionTokenBudget) Remaining(sessionID string) int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	spent := b.used[sessionID]
	if spent >= b.limit {
		return 0
	}
	return b.limit - spent
}

// TrySpend records usage when enough budget remains; returns false when over limit.
func (b *SessionTokenBudget) TrySpend(sessionID string, tokens int) bool {
	if b == nil || tokens <= 0 {
		return tokens <= 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	spent := b.used[sessionID]
	if spent+tokens > b.limit {
		return false
	}
	b.used[sessionID] = spent + tokens
	return true
}

// Spent returns total tokens charged to sessionID.
func (b *SessionTokenBudget) Spent(sessionID string) int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used[sessionID]
}
