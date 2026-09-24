package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client calls the runcloud9-ai HTTP API.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient targets baseURL (e.g. http://runcloud9-ai:5000) without a trailing slash.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 90 * time.Second,
		},
	}
}

// ReelCaptions requests caption claims for pre-selected highlight rows.
func (c *Client) ReelCaptions(ctx context.Context, req ReelCaptionsRequest) (*ReelCaptionsResponse, error) {
	if c == nil || c.baseURL == "" {
		return nil, fmt.Errorf("inference client not configured")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal reel captions request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/reel/captions", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build reel captions request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("reel captions request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read reel captions response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("reel captions status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out ReelCaptionsResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode reel captions response: %w", err)
	}
	return &out, nil
}

// Postmortem sends a session AIContext to runcloud9-ai for structured post-mortem claims.
func (c *Client) Postmortem(ctx context.Context, aiContext any) (*PostmortemResponse, error) {
	if c == nil || c.baseURL == "" {
		return nil, fmt.Errorf("inference client not configured")
	}
	body, err := json.Marshal(aiContext)
	if err != nil {
		return nil, fmt.Errorf("marshal postmortem request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/postmortem", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build postmortem request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("postmortem request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read postmortem response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("postmortem status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out PostmortemResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode postmortem response: %w", err)
	}
	return &out, nil
}
