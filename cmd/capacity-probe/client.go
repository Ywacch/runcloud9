package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

const (
	// apiHTTPTimeout is the whole-request budget for create, poll, and delete.
	// 60s expired while the edge was still holding DELETE (and slow POST) headers,
	// which left session namespaces behind after an otherwise successful wave.
	apiHTTPTimeout = 3 * time.Minute
)

type apiClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func newAPIClient(baseURL, token string) *apiClient {
	return &apiClient{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:      strings.TrimSpace(token),
		httpClient: newAPIHTTPClient(),
	}
}

func newAPIHTTPClient() *http.Client {
	var tr *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = base.Clone()
	} else {
		tr = &http.Transport{}
	}
	// Default MaxIdleConnsPerHost is 2. A 50-wide burst then opens and drops
	// connections on every call.
	tr.MaxIdleConns = 128
	tr.MaxIdleConnsPerHost = 64
	return &http.Client{
		Timeout:   apiHTTPTimeout,
		Transport: tr,
	}
}

type createResponse struct {
	DeploymentID string `json:"deploymentId"`
	Namespace    string `json:"namespace"`
	Status       string `json:"status"`
	Assessment   struct {
		Verdict     string `json:"verdict"`
		Explanation string `json:"explanation"`
	} `json:"assessment"`
	Error string `json:"error"`
}

type deploymentStatus struct {
	ID             string         `json:"id"`
	Namespace      string         `json:"namespace"`
	Template       string         `json:"template"`
	Archetype      string         `json:"archetype"`
	Status         string         `json:"status"`
	ExpiresAt      string         `json:"expiresAt"`
	CreatedAt      string         `json:"createdAt"`
	MetricsSummary map[string]any `json:"metricsSummary"`
	Error          string         `json:"error"`
}

type meResponse struct {
	Tier   string `json:"tier"`
	Limits struct {
		MaxConcurrent int `json:"maxConcurrent"`
	} `json:"limits"`
	Error string `json:"error"`
}

func (c *apiClient) GetMe(ctx context.Context) (meResponse, error) {
	var out meResponse
	if err := c.doJSON(ctx, http.MethodGet, "/me", nil, &out); err != nil {
		return meResponse{}, err
	}
	if out.Error != "" {
		return out, fmt.Errorf("GET /me: %s", out.Error)
	}
	return out, nil
}

func (c *apiClient) CreateDeployment(ctx context.Context, design deployer.DeploymentDesign) (createResponse, int, error) {
	body, err := json.Marshal(design)
	if err != nil {
		return createResponse{}, 0, err
	}
	var out createResponse
	status, err := c.doJSONStatus(ctx, http.MethodPost, "/deployments", body, &out)
	if err != nil {
		return createResponse{}, status, err
	}
	return out, status, nil
}

func (c *apiClient) GetDeployment(ctx context.Context, id string) (deploymentStatus, int, error) {
	var out deploymentStatus
	status, err := c.doJSONStatus(ctx, http.MethodGet, "/deployments/"+id, nil, &out)
	if err != nil {
		return deploymentStatus{}, status, err
	}
	return out, status, nil
}

func (c *apiClient) DeleteDeployment(ctx context.Context, id string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/deployments/"+id, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return resp.StatusCode, fmt.Errorf("DELETE /deployments/%s: status %d", id, resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func (c *apiClient) doJSON(ctx context.Context, method, path string, body []byte, out any) error {
	_, err := c.doJSONStatus(ctx, method, path, body, out)
	return err
}

func (c *apiClient) doJSONStatus(ctx context.Context, method, path string, body []byte, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPost && path == "/deployments" {
		req.Header.Set("X-Cloud9-Batch-Launch", "1")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if len(raw) > 0 && out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s %s: decode status %d: %w body=%s", method, path, resp.StatusCode, err, truncate(string(raw), 300))
		}
	}
	return resp.StatusCode, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
