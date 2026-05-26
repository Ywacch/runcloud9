package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ProxyClient talks to the caching-proxy HTTP API.
// It is used when CACHE_MODE=proxy.
type ProxyClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewProxyClient(cacheHost, proxyPort string) *ProxyClient {
	// cacheHost is a Kubernetes service DNS name (e.g. "cache").
	return &ProxyClient{
		baseURL:    fmt.Sprintf("http://%s:%s", cacheHost, proxyPort),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

type proxyGetResp struct {
	Value  string `json:"value"`
	Source string `json:"source"` // "cache" or "db"
}

func (c *ProxyClient) Get(ctx context.Context, code string) (value string, source string, ok bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/get/"+code, nil)
	if err != nil {
		return "", "", false, err
	}
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", "", false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", false, fmt.Errorf("proxy GET failed: status=%d", resp.StatusCode)
	}

	var out proxyGetResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", false, err
	}
	return out.Value, out.Source, true, nil
}

type proxySetReq struct {
	Value string `json:"value"`
	TTL   int    `json:"ttl"` // seconds
}
type proxyOkResp struct {
	Ok bool `json:"ok"`
}

func (c *ProxyClient) Set(ctx context.Context, code, value string, ttl time.Duration) error {
	payload := proxySetReq{
		Value: strings.TrimSpace(value),
		TTL:   int(ttl.Seconds()),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/set/"+code, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("proxy SET failed: status=%d", resp.StatusCode)
	}

	var out proxyOkResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if !out.Ok {
		return fmt.Errorf("proxy SET returned ok=false")
	}
	return nil
}

func (c *ProxyClient) Exists(ctx context.Context, code string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/exists/"+code, nil)
	if err != nil {
		return false, err
	}
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("proxy EXISTS failed: status=%d", resp.StatusCode)
	}

	var out struct {
		Exists bool `json:"exists"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, err
	}
	return out.Exists, nil
}

func (c *ProxyClient) Stats(ctx context.Context, code string) (URLStats, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/stats/"+code, nil)
	if err != nil {
		return URLStats{}, false, err
	}
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return URLStats{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return URLStats{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return URLStats{}, false, fmt.Errorf("proxy STATS failed: status=%d", resp.StatusCode)
	}

	var out struct {
		Code        string    `json:"code"`
		OriginalURL string    `json:"original_url"`
		CreatedAt   time.Time `json:"created_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return URLStats{}, false, err
	}

	return URLStats{
		Code:      out.Code,
		Original:  out.OriginalURL,
		CreatedAt: out.CreatedAt,
	}, true, nil
}

func (c *ProxyClient) Health(ctx context.Context) (status string, redis string, db string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return "", "", "", err
	}
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", "", fmt.Errorf("proxy HEALTH failed: status=%d", resp.StatusCode)
	}

	var out struct {
		Status string `json:"status"`
		Redis  string `json:"redis"`
		DB     string `json:"db"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", "", err
	}
	return out.Status, out.Redis, out.DB, nil
}

func (c *ProxyClient) Codes(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/codes", nil)
	if err != nil {
		return nil, err
	}
	setRequestIDHeader(req, ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy CODES failed: status=%d", resp.StatusCode)
	}

	var out struct {
		Codes []string `json:"codes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Codes, nil
}
