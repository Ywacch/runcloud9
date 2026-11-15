package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	postgrest "github.com/supabase-community/postgrest-go"

	"github.com/ywacch/cloud9/pkg/suparest"
)

type deploymentRow struct {
	Namespace      string         `json:"namespace"`
	Template       string         `json:"template"`
	Status         string         `json:"status"`
	CreatedAt      time.Time      `json:"created_at"`
	MetricsSummary map[string]any `json:"metrics_summary"`
}

type bucketStat struct {
	Name        string         `json:"name"`
	Total       int            `json:"total"`
	Successes   int            `json:"successes"`
	Failures    int            `json:"failures"`
	FailureRate float64        `json:"failure_rate"`
	Signatures  map[string]int `json:"top_signatures"`
}

type signatureInfo struct {
	Count        int `json:"count"`
	FirstOverlap int `json:"first_overlap"`
}

type report struct {
	AnalysisWindow             string                   `json:"analysis_window"`
	TotalDeployments           int                      `json:"total_deployments"`
	SafeSteadyStateConcurrency int                      `json:"safe_steady_state_concurrency"`
	CautionZone                string                   `json:"caution_zone"`
	UnsafeThreshold            int                      `json:"unsafe_threshold"`
	Buckets                    []bucketStat             `json:"buckets"`
	SignatureFrequency         map[string]signatureInfo `json:"signature_frequency"`
}

type deploymentSample struct {
	Row      deploymentRow
	Outcome  string
	EndAt    time.Time
	Overlap  int
	Failures []string
}

func main() {
	windowDays := flag.Int("days", 7, "analysis window in days")
	flag.Parse()

	url := strings.TrimSpace(os.Getenv("SUPABASE_URL"))
	key := strings.TrimSpace(os.Getenv("SUPABASE_SERVICE_KEY"))
	if url == "" || key == "" {
		log.Fatal("SUPABASE_URL and SUPABASE_SERVICE_KEY are required")
	}

	client, err := suparest.New(url, key)
	if err != nil {
		log.Fatalf("supabase client: %v", err)
	}

	rows, err := fetchRows(client, *windowDays)
	if err != nil {
		log.Fatalf("fetch rows: %v", err)
	}

	out := buildReport(rows, *windowDays, time.Now().UTC())
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		log.Fatalf("encode report: %v", err)
	}
}

func fetchRows(client *postgrest.Client, windowDays int) ([]deploymentRow, error) {
	cutoff := time.Now().UTC().Add(-time.Duration(windowDays) * 24 * time.Hour).Format(time.RFC3339)
	data, _, err := client.From("cloud_deployments").
		Select("namespace,template,status,created_at,metrics_summary", "exact", false).
		Gte("created_at", cutoff).
		Order("created_at", &postgrest.OrderOpts{Ascending: true}).
		Execute()
	if err != nil {
		return nil, fmt.Errorf("query deployments: %w", err)
	}
	var rows []deploymentRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("decode deployments: %w", err)
	}
	return rows, nil
}

func buildReport(rows []deploymentRow, windowDays int, now time.Time) report {
	samples := make([]deploymentSample, 0, len(rows))
	for _, r := range rows {
		diag := diagnosticsMap(r.MetricsSummary)
		outcome := stringFromMap(diag, "outcome")
		if outcome == "" {
			if r.Status == "failed" {
				outcome = "failed"
			} else {
				outcome = "success"
			}
		}
		endAt := endTimestamp(r, now)
		samples = append(samples, deploymentSample{
			Row:      r,
			Outcome:  outcome,
			EndAt:    endAt,
			Failures: failureSignatures(diag),
		})
	}

	for i := range samples {
		overlap := 0
		for j := range samples {
			if i == j {
				continue
			}
			if samples[j].Row.CreatedAt.After(samples[i].Row.CreatedAt) {
				continue
			}
			if !samples[j].EndAt.Before(samples[i].Row.CreatedAt) {
				overlap++
			}
		}
		samples[i].Overlap = overlap
	}

	buckets := []bucketStat{
		{Name: "0", Signatures: map[string]int{}},
		{Name: "1-2", Signatures: map[string]int{}},
		{Name: "3-4", Signatures: map[string]int{}},
		{Name: "5+", Signatures: map[string]int{}},
	}
	sigFreq := make(map[string]signatureInfo)
	for _, s := range samples {
		bi := bucketIndex(s.Overlap)
		b := &buckets[bi]
		b.Total++
		if s.Outcome == "failed" {
			b.Failures++
		} else {
			b.Successes++
		}
		for _, sig := range s.Failures {
			b.Signatures[sig]++
			info := sigFreq[sig]
			info.Count++
			if info.FirstOverlap == 0 || s.Overlap < info.FirstOverlap {
				info.FirstOverlap = s.Overlap
			}
			sigFreq[sig] = info
		}
	}
	for i := range buckets {
		if buckets[i].Total > 0 {
			buckets[i].FailureRate = float64(buckets[i].Failures) / float64(buckets[i].Total)
		}
	}

	safe := 0
	unsafe := 0
	for _, s := range samples {
		if s.Outcome != "failed" && s.Overlap >= safe {
			safe = s.Overlap
		}
	}
	for _, s := range samples {
		if s.Outcome == "failed" {
			if unsafe == 0 || s.Overlap < unsafe {
				unsafe = s.Overlap
			}
		}
	}
	caution := "n/a"
	if unsafe > safe+1 {
		caution = fmt.Sprintf("%d-%d concurrent", safe+1, unsafe-1)
	} else if unsafe > 0 {
		caution = fmt.Sprintf("%d concurrent", unsafe)
	}

	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })
	return report{
		AnalysisWindow:             fmt.Sprintf("last %d days", windowDays),
		TotalDeployments:           len(samples),
		SafeSteadyStateConcurrency: safe,
		CautionZone:                caution,
		UnsafeThreshold:            unsafe,
		Buckets:                    buckets,
		SignatureFrequency:         sigFreq,
	}
}

func diagnosticsMap(summary map[string]any) map[string]any {
	if summary == nil {
		return map[string]any{}
	}
	raw, ok := summary["diagnostics"]
	if !ok {
		return map[string]any{}
	}
	out, ok := raw.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return out
}

func failureSignatures(diag map[string]any) []string {
	raw, ok := diag["failure_signatures"]
	if !ok || raw == nil {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func endTimestamp(row deploymentRow, now time.Time) time.Time {
	if row.MetricsSummary != nil {
		if ts := stringFromMap(row.MetricsSummary, "terminated_at"); ts != "" {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				return t
			}
		}
	}
	if row.Status == "running" || row.Status == "provisioning" {
		return now
	}
	return row.CreatedAt
}

func stringFromMap(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func bucketIndex(overlap int) int {
	switch {
	case overlap <= 0:
		return 0
	case overlap <= 2:
		return 1
	case overlap <= 4:
		return 2
	default:
		return 3
	}
}
