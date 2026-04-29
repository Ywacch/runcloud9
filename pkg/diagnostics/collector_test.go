package diagnostics

import (
	"encoding/json"
	"testing"
)

func TestExtractSignatures_InsufficientMemory(t *testing.T) {
	t.Parallel()
	events := []EventSnapshot{{
		Reason:  "FailedScheduling",
		Message: "0/3 nodes are available: 3 Insufficient memory.",
	}}
	got := ExtractSignatures(events, "", map[string][]string{}, nil)
	if !hasCode(got, "SCHEDULING_INSUFFICIENT_MEMORY") {
		t.Fatalf("expected SCHEDULING_INSUFFICIENT_MEMORY, got %+v", got)
	}
}

func TestExtractSignatures_WorkerTimeout(t *testing.T) {
	t.Parallel()
	got := ExtractSignatures(nil, "timed out waiting for worker ready", map[string][]string{}, nil)
	if !hasCode(got, "WORKER_READY_TIMEOUT") {
		t.Fatalf("expected WORKER_READY_TIMEOUT, got %+v", got)
	}
}

func TestExtractSignatures_MultipleSources(t *testing.T) {
	t.Parallel()
	events := []EventSnapshot{{
		Reason:  "FailedScheduling",
		Message: "quota exceeded for SSD_TOTAL_GB",
	}}
	logs := map[string][]string{
		"api": {"loadgen pool empty for template social-feed"},
	}
	got := ExtractSignatures(events, "orchestration failed: timed out waiting for worker ready", logs, nil)
	if !hasCode(got, "SCHEDULING_QUOTA_EXCEEDED") || !hasCode(got, "LOADGEN_POOL_EMPTY") || !hasCode(got, "WORKER_READY_TIMEOUT") {
		t.Fatalf("expected multiple signatures, got %+v", got)
	}
}

func TestExtractSignatures_NoSignatures(t *testing.T) {
	t.Parallel()
	got := ExtractSignatures(nil, "", map[string][]string{"x": {"healthy deployment"}}, nil)
	if len(got) != 0 {
		t.Fatalf("expected no signatures, got %+v", got)
	}
}

func TestRedactString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "bearer", in: "Authorization: Bearer eyJabc123", want: "Authorization: Bearer [REDACTED]"},
		{name: "password", in: "password=secret123", want: "password=[REDACTED]"},
		{name: "token", in: "token=abc.def", want: "token=[REDACTED]"},
		{name: "clean", in: "all good", want: "all good"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RedactString(tc.in); got != tc.want {
				t.Fatalf("RedactString(%q)=%q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDiagnosticArtifact_ValidJSON(t *testing.T) {
	t.Parallel()
	artifact := DiagnosticArtifact{
		SchemaVersion: 1,
		CollectedAt:   "2026-04-29T00:00:00Z",
		Namespace:     "cloud9-test",
		Template:      "ecommerce",
		Outcome:       "success",
		Logs:          map[string][]string{"pod/container": {"ok"}},
	}
	body, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded DiagnosticArtifact
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Namespace != artifact.Namespace || decoded.SchemaVersion != 1 {
		t.Fatalf("unexpected decoded artifact: %+v", decoded)
	}
}

func hasCode(sigs []FailureSignature, code string) bool {
	for _, s := range sigs {
		if s.Code == code {
			return true
		}
	}
	return false
}
