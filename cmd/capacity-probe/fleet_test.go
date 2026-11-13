package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestWavePoll_UsesFleetList(t *testing.T) {
	var created atomic.Int32
	var fleetCalls atomic.Int32
	var getCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/deployments" {
			n := created.Add(1)
			id := fmt.Sprintf("id-%d", n)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"deploymentId": id,
				"namespace":    id,
				"status":       "provisioning",
			})
			return
		}
		if r.URL.Path == "/internal/fleet" {
			n := fleetCalls.Add(1)
			status := "provisioning"
			if n >= 2 {
				status = "running"
			}
			items := make([]fleetItem, 0, 3)
			for i := 1; i <= 3; i++ {
				id := fmt.Sprintf("id-%d", i)
				items = append(items, fleetItem{ID: id, Namespace: id, Status: status})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
			return
		}
		getCalls.Add(1)
		http.NotFound(w, r)
	}))
	defer srv.Close()

	client := newAPIClient(srv.URL, "token")
	every := 15 * time.Millisecond
	hub := newFleetHub(client, every)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.run(ctx)

	tracker := &deploymentTracker{}
	done := make(chan DeploymentResult, 3)
	for i := 0; i < 3; i++ {
		go func() {
			done <- createAndWait(ctx, client, tracker, plannedDeploy{
				body: deployer.DeploymentDesign{},
			}, every, 2*time.Second, hub)
		}()
	}
	for i := 0; i < 3; i++ {
		select {
		case res := <-done:
			if res.TerminalStatus != "running" {
				t.Fatalf("status %s reason %s", res.TerminalStatus, res.FailureReason)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for fleet poll")
		}
	}
	if fleetCalls.Load() < 2 {
		t.Fatalf("fleet calls = %d, want at least 2", fleetCalls.Load())
	}
	if getCalls.Load() != 0 {
		t.Fatalf("GET /deployments calls = %d, want 0", getCalls.Load())
	}
}

func TestCreateAndWait_NotFoundIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"deploymentId": "ns-1",
				"namespace":    "ns-1",
				"status":       "provisioning",
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}))
	defer srv.Close()

	res := createAndWait(context.Background(), newAPIClient(srv.URL, "token"), &deploymentTracker{}, plannedDeploy{
		body: deployer.DeploymentDesign{},
	}, 10*time.Millisecond, time.Second, nil)
	if res.TerminalStatus != "terminated" {
		t.Fatalf("status = %s reason %s", res.TerminalStatus, res.FailureReason)
	}
	if res.FailureReason != "deployment disappeared while provisioning" {
		t.Fatalf("reason = %s", res.FailureReason)
	}
}
