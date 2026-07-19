package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// CampaignReport is the full JSON output of a capacity campaign.
type CampaignReport struct {
	Name       string       `json:"name"`
	Tier       string       `json:"tier"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt time.Time    `json:"finished_at"`
	DryRun     bool         `json:"dry_run"`
	Waves      []WaveReport `json:"waves"`
}

// WaveReport summarizes one concurrency wave.
type WaveReport struct {
	Name                 string             `json:"name"`
	Mode                 string             `json:"mode"`
	RequestedCount       int                `json:"requested_count"`
	AdmittedCount        int                `json:"admitted_count"`
	SuccessCount         int                `json:"success_count"`
	FailedCount          int                `json:"failed_count"`
	RejectedCount        int                `json:"rejected_count"`
	SuccessRate          float64            `json:"success_rate"`
	RequestedWeight      DeploymentWeight   `json:"requested_weight"`
	AdmittedWeight       DeploymentWeight   `json:"admitted_weight"`
	SuccessfulWeight     DeploymentWeight   `json:"successful_weight"`
	PeakSimultaneousProv int                `json:"peak_simultaneous_provisions"`
	P50TimeToRunningMS   int64              `json:"p50_time_to_running_ms"`
	P95TimeToRunningMS   int64              `json:"p95_time_to_running_ms"`
	FailureBreakdown     map[string]int     `json:"failure_breakdown"`
	Deployments          []DeploymentResult `json:"deployments"`
	ClusterSamples       []ClusterSample    `json:"cluster_samples,omitempty"`
}

type timeInterval struct {
	start time.Time
	end   time.Time
}

// DeploymentResult is one attempted deployment inside a wave.
type DeploymentResult struct {
	Index           int              `json:"index"`
	Design          string           `json:"design"`
	Weight          DeploymentWeight `json:"weight"`
	HTTPStatus      int              `json:"http_status"`
	DeploymentID    string           `json:"deployment_id,omitempty"`
	Namespace       string           `json:"namespace,omitempty"`
	CreateStatus    string           `json:"create_status,omitempty"`
	TerminalStatus  string           `json:"terminal_status,omitempty"`
	FailureReason   string           `json:"failure_reason,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	TerminalAt      time.Time        `json:"terminal_at,omitempty"`
	TimeToRunningMS int64            `json:"time_to_running_ms,omitempty"`
	ProvisionPhases []string         `json:"provision_phases,omitempty"`
}

func summarizeWave(name, mode string, results []DeploymentResult, samples []ClusterSample) WaveReport {
	rep := WaveReport{
		Name:             name,
		Mode:             mode,
		RequestedCount:   len(results),
		FailureBreakdown: map[string]int{},
		Deployments:      results,
		ClusterSamples:   samples,
	}
	var durations []int64
	var intervals []timeInterval

	for _, r := range results {
		rep.RequestedWeight = addWeight(rep.RequestedWeight, r.Weight)
		if r.DeploymentID != "" && r.CreateStatus != "simulation_recommended" && r.HTTPStatus == 201 {
			rep.AdmittedCount++
			rep.AdmittedWeight = addWeight(rep.AdmittedWeight, r.Weight)
			end := r.TerminalAt
			if end.IsZero() {
				end = r.CreatedAt.Add(time.Millisecond)
			}
			intervals = append(intervals, timeInterval{start: r.CreatedAt, end: end})
		} else if r.HTTPStatus == 429 || r.CreateStatus == "rejected" || r.HTTPStatus >= 400 {
			rep.RejectedCount++
			reason := r.FailureReason
			if reason == "" {
				reason = fmt.Sprintf("http_%d", r.HTTPStatus)
			}
			rep.FailureBreakdown[reason]++
		}
		switch r.TerminalStatus {
		case "running":
			rep.SuccessCount++
			rep.SuccessfulWeight = addWeight(rep.SuccessfulWeight, r.Weight)
			if r.TimeToRunningMS > 0 {
				durations = append(durations, r.TimeToRunningMS)
			}
		case "failed":
			rep.FailedCount++
			reason := r.FailureReason
			if reason == "" {
				reason = "failed"
			}
			rep.FailureBreakdown[reason]++
		}
	}
	if rep.RequestedCount > 0 {
		rep.SuccessRate = float64(rep.SuccessCount) / float64(rep.RequestedCount)
	}
	rep.P50TimeToRunningMS = percentile(durations, 50)
	rep.P95TimeToRunningMS = percentile(durations, 95)
	rep.PeakSimultaneousProv = peakSimultaneous(intervals)
	return rep
}

func peakSimultaneous(intervals []timeInterval) int {
	if len(intervals) == 0 {
		return 0
	}
	type edge struct {
		t time.Time
		d int
	}
	edges := make([]edge, 0, len(intervals)*2)
	for _, iv := range intervals {
		edges = append(edges, edge{t: iv.start, d: 1}, edge{t: iv.end, d: -1})
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].t.Equal(edges[j].t) {
			return edges[i].d < edges[j].d // ends before starts at same timestamp
		}
		return edges[i].t.Before(edges[j].t)
	})
	cur, peak := 0, 0
	for _, e := range edges {
		cur += e.d
		if cur > peak {
			peak = cur
		}
	}
	return peak
}

func percentile(values []int64, p int) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (p * (len(sorted) - 1)) / 100
	return sorted[idx]
}

func writeJSONReport(path string, report CampaignReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func writeCSVSummary(path string, report CampaignReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{
		"wave", "mode", "requested", "admitted", "success", "failed", "rejected",
		"success_rate", "req_cpu_m", "req_mem_mi", "ok_cpu_m", "ok_mem_mi",
		"peak_provisions", "p50_ms", "p95_ms",
	})
	for _, wave := range report.Waves {
		_ = w.Write([]string{
			wave.Name,
			wave.Mode,
			strconv.Itoa(wave.RequestedCount),
			strconv.Itoa(wave.AdmittedCount),
			strconv.Itoa(wave.SuccessCount),
			strconv.Itoa(wave.FailedCount),
			strconv.Itoa(wave.RejectedCount),
			fmt.Sprintf("%.3f", wave.SuccessRate),
			strconv.FormatInt(wave.RequestedWeight.CPUMillicores, 10),
			strconv.FormatInt(wave.RequestedWeight.MemoryMi, 10),
			strconv.FormatInt(wave.SuccessfulWeight.CPUMillicores, 10),
			strconv.FormatInt(wave.SuccessfulWeight.MemoryMi, 10),
			strconv.Itoa(wave.PeakSimultaneousProv),
			strconv.FormatInt(wave.P50TimeToRunningMS, 10),
			strconv.FormatInt(wave.P95TimeToRunningMS, 10),
		})
	}
	return w.Error()
}
