package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
)

// PodLoadGenerator drives load via the per-namespace loadgen pod HTTP API.
type PodLoadGenerator struct {
	httpClient *http.Client
}

var _ LoadGenerator = (*PodLoadGenerator)(nil)

// podEndpointOverride is set in tests to point at httptest servers.
var podEndpointOverride func(namespace string) string

// NewPodLoadGenerator returns a load generator that talks to in-cluster loadgen pods.
func NewPodLoadGenerator(_ kubernetes.Interface) *PodLoadGenerator {
	return &PodLoadGenerator{
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func podEndpoint(namespace string) string {
	if podEndpointOverride != nil {
		return podEndpointOverride(namespace)
	}
	return fmt.Sprintf("http://loadgen.%s.svc.cluster.local:9100", namespace)
}

// Start resolves targets and POSTs /start on the namespace loadgen pod.
func (p *PodLoadGenerator) Start(ctx context.Context, namespace string, profile LoadProfile) error {
	profile = NormalizeProfile(profile)
	body, err := BuildPodStartBody(ctx, namespace, profile)
	if err != nil {
		return err
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("loadgen pod: encode start: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, podEndpoint(namespace)+"/start", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("loadgen pod: build start request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("loadgen pod: start request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("loadgen pod: attack already running")
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("loadgen pod: start returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

type rateUpdateBody struct {
	RPS int `json:"rps"`
}

// Update PATCHes /rate on the namespace loadgen pod.
func (p *PodLoadGenerator) Update(ctx context.Context, namespace string, rps int) error {
	if err := ValidateLiveRPS(rps); err != nil {
		return err
	}
	raw, err := json.Marshal(rateUpdateBody{RPS: rps})
	if err != nil {
		return fmt.Errorf("loadgen pod: encode rate: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, podEndpoint(namespace)+"/rate", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("loadgen pod: build rate request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("loadgen pod: rate request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return ErrNotRunning
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("loadgen pod: rate returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// Stop sends DELETE /stop on the namespace loadgen pod.
func (p *PodLoadGenerator) Stop(ctx context.Context, namespace string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, podEndpoint(namespace)+"/stop", nil)
	if err != nil {
		return fmt.Errorf("loadgen pod: build stop request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("loadgen pod: stop request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("loadgen pod: stop returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// GetStats fetches GET /metrics from the loadgen pod.
func (p *PodLoadGenerator) GetStats(namespace string) LoadStats {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, podEndpoint(namespace)+"/metrics", nil)
	if err != nil {
		return inactiveLoadStats()
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return inactiveLoadStats()
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return inactiveLoadStats()
	}

	var stats LoadStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return inactiveLoadStats()
	}
	if stats.StatusCodes == nil {
		stats.StatusCodes = map[string]int{}
	}
	if stats.Distribution == nil {
		stats.Distribution = map[string]int{}
	}
	return stats
}

func inactiveLoadStats() LoadStats {
	return LoadStats{
		Active:       false,
		StatusCodes:  map[string]int{},
		Distribution: map[string]int{},
	}
}
