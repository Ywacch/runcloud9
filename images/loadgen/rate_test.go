package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

func TestHandleRate_notRunning(t *testing.T) {
	s := &server{
		stats: loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/rate", strings.NewReader(`{"rps":20}`))
	s.handleRate(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status: want 409, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleRate_running(t *testing.T) {
	s := &server{
		running: true,
		control: loadgen.NewAttackControl(10),
		stats:   loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/rate", strings.NewReader(`{"rps":40}`))
	s.handleRate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if s.control.BaseRPS() != 40 {
		t.Fatalf("base rps: got %d want 40", s.control.BaseRPS())
	}
}

func TestHandleRate_badRPS(t *testing.T) {
	s := &server{
		running: true,
		control: loadgen.NewAttackControl(10),
		stats:   loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/rate", strings.NewReader(`{"rps":0}`))
	s.handleRate(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleRate_mixOnly(t *testing.T) {
	s := &server{
		running: true,
		control: loadgen.NewAttackControl(10),
		allowedDistKeys: map[string]struct{}{
			"GET /r/{code}": {},
			"POST /shorten": {},
		},
		stats: loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/rate", strings.NewReader(`{"distribution":{"GET /r/{code}":50,"POST /shorten":50}}`))
	s.handleRate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	got := s.control.BaseDistribution()
	if got["GET /r/{code}"] != 50 || got["POST /shorten"] != 50 {
		t.Fatalf("distribution: got %+v", got)
	}
}

func TestHandleRate_emptyBody(t *testing.T) {
	s := &server{
		running: true,
		control: loadgen.NewAttackControl(10),
		stats:   loadgen.LoadStats{StatusCodes: map[string]int{}, Distribution: map[string]int{}},
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/rate", strings.NewReader(`{}`))
	s.handleRate(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}
