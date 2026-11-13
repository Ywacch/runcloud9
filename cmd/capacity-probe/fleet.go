package main

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// fleetHub polls GET /internal/fleet on one goroutine.
// unavailable is set when the route answers 404.
type fleetHub struct {
	client *apiClient
	every  time.Duration

	mu          sync.Mutex
	byID        map[string]string
	gen         int
	unavailable bool
}

func newFleetHub(client *apiClient, every time.Duration) *fleetHub {
	if every <= 0 {
		every = 2 * time.Second
	}
	return &fleetHub{
		client: client,
		every:  every,
		byID:   map[string]string{},
	}
}

func (h *fleetHub) run(ctx context.Context) {
	if h == nil {
		return
	}
	h.poll(ctx)
	ticker := time.NewTicker(h.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.poll(ctx)
		}
	}
}

func (h *fleetHub) poll(ctx context.Context) {
	items, code, err := h.client.ListFleet(ctx)
	h.mu.Lock()
	defer h.mu.Unlock()
	if code == http.StatusNotFound {
		h.unavailable = true
		return
	}
	if err != nil {
		return
	}
	next := make(map[string]string, len(items))
	for _, item := range items {
		if item.ID != "" {
			next[item.ID] = item.Status
		}
		if item.Namespace != "" {
			next[item.Namespace] = item.Status
		}
	}
	h.byID = next
	h.gen++
}

// snapshot reports the latest poll. present is false when id is absent.
// gen 0 means no successful poll yet.
func (h *fleetHub) snapshot(id string) (status string, gen int, present, unavailable bool) {
	if h == nil {
		return "", 0, false, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unavailable {
		return "", h.gen, false, true
	}
	status, present = h.byID[id]
	return status, h.gen, present, false
}

func (h *fleetHub) view() (ids map[string]string, gen int, unavailable bool) {
	if h == nil {
		return nil, 0, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(h.byID))
	for id, status := range h.byID {
		out[id] = status
	}
	return out, h.gen, h.unavailable
}
