// Command capacity-probe runs controlled concurrent-deployment campaigns against
// a live Cloud9 API to measure cluster capacity for the admission bouncer.
//
// It creates real GKE namespaces and costs real money. Unit tests never call
// the API; only an explicit binary invocation with -confirm does.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("capacity-probe: ")

	manifestPath := flag.String("manifest", "", "path to wave manifest YAML")
	apiURL := flag.String("api-url", envOr("CAPACITY_PROBE_API_URL", ""), "Cloud9 API base URL")
	token := flag.String("token", envOr("CAPACITY_PROBE_TOKEN", ""), "Bearer JWT for an internal account")
	outDir := flag.String("out", "capacity-probe-results", "directory for JSON/CSV reports")
	pollEvery := flag.Duration("poll", 2500*time.Millisecond, "deployment status poll interval")
	timeout := flag.Duration("timeout", 6*time.Minute, "per-deployment wait for running/failed")
	dryRun := flag.Bool("dry-run", false, "validate fixtures and print weights without calling the API")
	confirm := flag.Bool("confirm", false, "required to create real deployments (ignored with -dry-run)")
	sampleCluster := flag.Bool("sample-cluster", false, "sample Kubernetes pressure during waves")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path for -sample-cluster (defaults to KUBECONFIG or ~/.kube/config)")
	flag.Parse()

	if strings.TrimSpace(*manifestPath) == "" {
		fmt.Fprintln(os.Stderr, "usage: capacity-probe -manifest <path> [-dry-run | -confirm] ...")
		flag.PrintDefaults()
		os.Exit(2)
	}

	manifest, err := loadManifest(*manifestPath)
	if err != nil {
		log.Fatalf("manifest: %v", err)
	}
	baseDir := filepath.Dir(*manifestPath)
	tier := manifest.effectiveTier()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	report := CampaignReport{
		Name:      manifest.Name,
		Tier:      tier,
		StartedAt: time.Now().UTC(),
		DryRun:    *dryRun,
	}

	if *dryRun {
		for _, wave := range manifest.Waves {
			wr, err := dryRunWave(wave, baseDir, tier)
			if err != nil {
				log.Fatalf("wave %s: %v", wave.Name, err)
			}
			report.Waves = append(report.Waves, wr)
		}
		report.FinishedAt = time.Now().UTC()
		printDryRunSummary(report)
		writeReports(*outDir, report)
		return
	}

	if !*confirm {
		log.Fatal("refusing to create real deployments without -confirm (use -dry-run to validate)")
	}
	if strings.TrimSpace(*apiURL) == "" {
		log.Fatal("-api-url or CAPACITY_PROBE_API_URL is required")
	}
	if strings.TrimSpace(*token) == "" {
		log.Fatal("-token or CAPACITY_PROBE_TOKEN is required")
	}

	client := newAPIClient(*apiURL, *token)
	if err := ensureMeAllows(ctx, client, largestWaveCount(manifest)); err != nil {
		log.Fatal(err)
	}

	tracker := &deploymentTracker{}
	defer func() {
		// DELETE returns after the row write. The poll waits until terminated.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()
		cleanupTracked(cleanupCtx, client, tracker)
	}()

	for i, wave := range manifest.Waves {
		select {
		case <-ctx.Done():
			log.Printf("canceled before wave %s", wave.Name)
			report.FinishedAt = time.Now().UTC()
			writeReports(*outDir, report)
			return
		default:
		}

		plans, total, err := planWave(wave, baseDir, tier)
		if err != nil {
			log.Fatalf("wave %s: %v", wave.Name, err)
		}
		log.Printf("starting wave %s count=%d cpu=%dm mem=%dMi mode=%s",
			wave.Name, len(plans), total.CPUMillicores, total.MemoryMi, wave.effectiveMode())

		var sampler *clusterSampler
		if *sampleCluster {
			sampler, err = newClusterSampler(*kubeconfig, 2*time.Second)
			if err != nil {
				log.Fatalf("cluster sampler: %v", err)
			}
		}

		wr := runWave(ctx, client, tracker, wave, plans, *pollEvery, *timeout, sampler)
		report.Waves = append(report.Waves, wr)
		log.Printf("wave %s done success=%d/%d peak_provisions=%d p50=%dms p95=%dms",
			wr.Name, wr.SuccessCount, wr.RequestedCount, wr.PeakSimultaneousProv, wr.P50TimeToRunningMS, wr.P95TimeToRunningMS)

		cleanupTracked(ctx, client, tracker)
		tracker = &deploymentTracker{}

		if i < len(manifest.Waves)-1 {
			if cd := manifest.cooldown(); cd > 0 {
				log.Printf("cooldown %s before next wave", cd)
				select {
				case <-ctx.Done():
					report.FinishedAt = time.Now().UTC()
					writeReports(*outDir, report)
					return
				case <-time.After(cd):
				}
			}
		}
	}

	report.FinishedAt = time.Now().UTC()
	writeReports(*outDir, report)
	log.Printf("campaign complete; reports in %s", *outDir)
}

func writeReports(outDir string, report CampaignReport) {
	stamp := report.StartedAt.Format("20060102-150405")
	base := filepath.Join(outDir, sanitizeFilename(report.Name)+"-"+stamp)
	jsonPath := base + ".json"
	csvPath := base + ".csv"
	if err := writeJSONReport(jsonPath, report); err != nil {
		log.Fatalf("write json: %v", err)
	}
	if err := writeCSVSummary(csvPath, report); err != nil {
		log.Fatalf("write csv: %v", err)
	}
	log.Printf("wrote %s", jsonPath)
	log.Printf("wrote %s", csvPath)
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func sanitizeFilename(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "campaign"
	}
	replacer := strings.NewReplacer("/", "-", "\\", "-", " ", "-", ":", "-")
	return replacer.Replace(s)
}
