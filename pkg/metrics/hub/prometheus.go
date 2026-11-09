package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// HTTPQuerier runs instant queries against a Prometheus HTTP API.
type HTTPQuerier struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (q *HTTPQuerier) Query(ctx context.Context, promql string) ([]Sample, error) {
	if q == nil || q.BaseURL == "" {
		return nil, fmt.Errorf("hub: prometheus url not configured")
	}
	client := q.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	endpoint := q.BaseURL + "/api/v1/query"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+url.Values{"query": {promql}}.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hub: prometheus query: %w", err)
	}
	defer resp.Body.Close()

	var pr promAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("hub: decode prometheus response: %w", err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("hub: prometheus status %q", pr.Status)
	}
	out := make([]Sample, 0, len(pr.Data.Result))
	for i := range pr.Data.Result {
		r := &pr.Data.Result[i]
		v, ok := parsePromValue(r.Value[1])
		if !ok {
			continue
		}
		out = append(out, Sample{
			Namespace: r.Metric["namespace"],
			App:       r.Metric["app"],
			Pod:       r.Metric["pod"],
			Le:        r.Metric["le"],
			Value:     v,
		})
	}
	return out, nil
}

type promAPIResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string  `json:"metric"`
			Value  [2]json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func parsePromValue(raw json.RawMessage) (float64, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}
