package store

import (
	"context"
	"fmt"
)

// Admission event kinds written by the API. Schema lives in Supabase
// public.admission_events; see docs/concurrency-analysis.md for query recipes.
const (
	AdmissionKindRoomQueued   = "room_queued"
	AdmissionKindRoomPromoted = "room_promoted"
	AdmissionKindRoomExpired  = "room_expired"
	AdmissionKindDoorWait     = "door_wait"
	AdmissionKindDoorAcquired = "door_acquired"
	AdmissionKindQueueFull    = "queue_full"
)

// AdmissionEvent is one room/door wait or queue_full row.
type AdmissionEvent struct {
	UserID        string
	Namespace     string
	Template      string
	Kind          string
	WaitMS        *int
	QueuePosition *int
	Paid          bool
	Detail        map[string]any
}

// ValidAdmissionKind reports whether kind is a value the table CHECK allows.
func ValidAdmissionKind(kind string) bool {
	switch kind {
	case AdmissionKindRoomQueued,
		AdmissionKindRoomPromoted,
		AdmissionKindRoomExpired,
		AdmissionKindDoorWait,
		AdmissionKindDoorAcquired,
		AdmissionKindQueueFull:
		return true
	default:
		return false
	}
}

// InsertAdmissionEvent appends one wait/reject row. Best-effort callers should
// log the error and continue; admit/provision must not fail on analytics.
func (s *DeploymentStore) InsertAdmissionEvent(ctx context.Context, ev AdmissionEvent) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store.InsertAdmissionEvent: %w", err)
	}
	if s == nil || s.client == nil {
		return fmt.Errorf("store.InsertAdmissionEvent: store unavailable")
	}
	if !ValidAdmissionKind(ev.Kind) {
		return fmt.Errorf("store.InsertAdmissionEvent: unknown kind %q", ev.Kind)
	}
	row := map[string]any{
		"kind":     ev.Kind,
		"template": ev.Template,
		"paid":     ev.Paid,
	}
	if ev.UserID != "" {
		row["user_id"] = ev.UserID
	}
	if ev.Namespace != "" {
		row["namespace"] = ev.Namespace
	}
	if ev.WaitMS != nil {
		row["wait_ms"] = *ev.WaitMS
	}
	if ev.QueuePosition != nil {
		row["queue_position"] = *ev.QueuePosition
	}
	if ev.Detail != nil {
		row["detail"] = ev.Detail
	}
	s.observe("insert_admission_event")
	_, _, err := s.client.From("admission_events").Insert([]map[string]any{row}, false, "", "", "").Execute()
	if err != nil {
		return fmt.Errorf("store.InsertAdmissionEvent: %w", err)
	}
	return nil
}
