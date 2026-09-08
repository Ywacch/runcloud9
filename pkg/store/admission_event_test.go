package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidAdmissionKind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind string
		want bool
	}{
		{name: "room_queued", kind: AdmissionKindRoomQueued, want: true},
		{name: "room_promoted", kind: AdmissionKindRoomPromoted, want: true},
		{name: "room_expired", kind: AdmissionKindRoomExpired, want: true},
		{name: "door_wait", kind: AdmissionKindDoorWait, want: true},
		{name: "door_acquired", kind: AdmissionKindDoorAcquired, want: true},
		{name: "queue_full", kind: AdmissionKindQueueFull, want: true},
		{name: "unknown", kind: "concurrent_active", want: false},
		{name: "empty", kind: "", want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ValidAdmissionKind(tt.kind); got != tt.want {
				t.Fatalf("ValidAdmissionKind(%q) = %v want %v", tt.kind, got, tt.want)
			}
		})
	}
}

func TestInsertAdmissionEvent(t *testing.T) {
	t.Parallel()
	waitMS := 1500
	pos := 2
	tests := []struct {
		name    string
		ev      AdmissionEvent
		cancel  bool
		wantErr bool
		check   func(t *testing.T, inserted map[string]any)
	}{
		{
			name: "queue_full omits namespace",
			ev: AdmissionEvent{
				UserID:   "user-1",
				Template: "url-shortener",
				Kind:     AdmissionKindQueueFull,
				Paid:     true,
			},
			check: func(t *testing.T, inserted map[string]any) {
				t.Helper()
				if inserted["kind"] != AdmissionKindQueueFull {
					t.Fatalf("kind = %v", inserted["kind"])
				}
				if inserted["user_id"] != "user-1" {
					t.Fatalf("user_id = %v", inserted["user_id"])
				}
				if _, ok := inserted["namespace"]; ok {
					t.Fatalf("namespace present: %v", inserted["namespace"])
				}
				if inserted["paid"] != true {
					t.Fatalf("paid = %v", inserted["paid"])
				}
			},
		},
		{
			name: "room_promoted includes wait and position",
			ev: AdmissionEvent{
				UserID:        "user-2",
				Namespace:     "ns-1",
				Template:      "social-feed",
				Kind:          AdmissionKindRoomPromoted,
				WaitMS:        &waitMS,
				QueuePosition: &pos,
				Paid:          false,
				Detail:        map[string]any{"reason": "promoted"},
			},
			check: func(t *testing.T, inserted map[string]any) {
				t.Helper()
				if inserted["namespace"] != "ns-1" {
					t.Fatalf("namespace = %v", inserted["namespace"])
				}
				assertJSONInt64(t, inserted["wait_ms"], 1500)
				assertJSONInt64(t, inserted["queue_position"], 2)
			},
		},
		{
			name:    "unknown kind",
			ev:      AdmissionEvent{Kind: "nope"},
			wantErr: true,
		},
		{
			name:    "canceled context",
			ev:      AdmissionEvent{Kind: AdmissionKindQueueFull},
			cancel:  true,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var inserted []map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/rest/v1/admission_events" {
					t.Errorf("path = %q", r.URL.Path)
				}
				if err := json.NewDecoder(r.Body).Decode(&inserted); err != nil {
					t.Errorf("decode insert body: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]map[string]any{})
			}))
			defer server.Close()

			s, err := NewDeploymentStore(server.URL, "service-key")
			if err != nil {
				t.Fatalf("NewDeploymentStore: %v", err)
			}
			ctx := context.Background()
			if tt.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err = s.InsertAdmissionEvent(ctx, tt.ev)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("InsertAdmissionEvent: %v", err)
			}
			if len(inserted) != 1 {
				t.Fatalf("inserted rows = %d", len(inserted))
			}
			if tt.check != nil {
				tt.check(t, inserted[0])
			}
		})
	}
}
