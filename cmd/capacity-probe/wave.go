package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

type trackedDeployment struct {
	ID        string
	Namespace string
}

type deploymentTracker struct {
	mu   sync.Mutex
	list []trackedDeployment
}

func (t *deploymentTracker) add(id, namespace string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.list = append(t.list, trackedDeployment{ID: id, Namespace: namespace})
}

func (t *deploymentTracker) snapshot() []trackedDeployment {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]trackedDeployment, len(t.list))
	copy(out, t.list)
	return out
}

type plannedDeploy struct {
	index  int
	design string
	body   deployer.DeploymentDesign
	weight DeploymentWeight
}

func planWave(wave Wave, baseDir, tier string) ([]plannedDeploy, DeploymentWeight, error) {
	var plans []plannedDeploy
	var total DeploymentWeight
	idx := 0
	for _, g := range wave.Groups {
		design, err := resolveDesign(g.Design, baseDir)
		if err != nil {
			return nil, DeploymentWeight{}, err
		}
		w := computeWeight(design, tier)
		for i := 0; i < g.Count; i++ {
			plans = append(plans, plannedDeploy{
				index:  idx,
				design: g.Design,
				body:   design,
				weight: w,
			})
			total = addWeight(total, w)
			idx++
		}
	}
	return plans, total, nil
}

func runWave(ctx context.Context, client *apiClient, tracker *deploymentTracker, wave Wave, plans []plannedDeploy, pollEvery, timeout time.Duration, sampler *clusterSampler) WaveReport {
	mode := wave.effectiveMode()
	results := make([]DeploymentResult, len(plans))
	var wg sync.WaitGroup

	// No-op by default so defer always has a cancel on every path (go vet).
	sampleCancel := func() {}
	if sampler != nil {
		var sampleCtx context.Context
		sampleCtx, sampleCancel = context.WithCancel(ctx)
		go sampler.run(sampleCtx)
	}
	defer sampleCancel()

	startOne := func(p plannedDeploy) {
		defer wg.Done()
		results[p.index] = createAndWait(ctx, client, tracker, p, pollEvery, timeout)
	}

	switch mode {
	case "paced":
		pace := time.Duration(wave.PaceMS) * time.Millisecond
		for _, p := range plans {
			select {
			case <-ctx.Done():
				results[p.index] = DeploymentResult{
					Index:         p.index,
					Design:        p.design,
					Weight:        p.weight,
					CreatedAt:     time.Now().UTC(),
					FailureReason: "campaign canceled",
					CreateStatus:  "rejected",
				}
				continue
			default:
			}
			wg.Add(1)
			go startOne(p)
			if pace > 0 {
				time.Sleep(pace)
			}
		}
	default: // burst
		for _, p := range plans {
			wg.Add(1)
			go startOne(p)
		}
	}
	wg.Wait()

	var samples []ClusterSample
	if sampler != nil {
		sampleCancel() // stop sampler before snapshot; defer is idempotent
		samples = sampler.snapshot()
	}
	return summarizeWave(wave.Name, mode, results, samples)
}

func createAndWait(ctx context.Context, client *apiClient, tracker *deploymentTracker, p plannedDeploy, pollEvery, timeout time.Duration) DeploymentResult {
	res := DeploymentResult{
		Index:     p.index,
		Design:    p.design,
		Weight:    p.weight,
		CreatedAt: time.Now().UTC(),
	}
	created, status, err := client.CreateDeployment(ctx, p.body)
	res.HTTPStatus = status
	if isAuthFailure(status) {
		res.FailureReason = "authentication failed (token expired or forbidden)"
		res.CreateStatus = "rejected"
		res.TerminalAt = time.Now().UTC()
		return res
	}
	if err != nil {
		res.FailureReason = err.Error()
		res.CreateStatus = "rejected"
		res.TerminalAt = time.Now().UTC()
		return res
	}
	res.CreateStatus = created.Status
	res.DeploymentID = created.DeploymentID
	res.Namespace = created.Namespace
	if created.Error != "" && created.DeploymentID == "" {
		res.FailureReason = created.Error
		res.CreateStatus = "rejected"
		res.TerminalAt = time.Now().UTC()
		return res
	}
	if status == http.StatusOK && created.Status == "simulation_recommended" {
		res.FailureReason = created.Assessment.Explanation
		if res.FailureReason == "" {
			res.FailureReason = "simulation_recommended"
		}
		res.TerminalStatus = "simulation_recommended"
		res.TerminalAt = time.Now().UTC()
		return res
	}
	if status != http.StatusCreated || created.DeploymentID == "" {
		res.FailureReason = created.Error
		if res.FailureReason == "" {
			res.FailureReason = fmt.Sprintf("unexpected create status %d", status)
		}
		res.CreateStatus = "rejected"
		res.TerminalAt = time.Now().UTC()
		return res
	}
	tracker.add(created.DeploymentID, created.Namespace)

	deadline := time.Now().Add(timeout)
	phases := map[string]struct{}{}
	for {
		if time.Now().After(deadline) {
			res.TerminalStatus = "failed"
			res.FailureReason = "poll timeout waiting for terminal status"
			res.TerminalAt = time.Now().UTC()
			res.ProvisionPhases = phaseList(phases)
			return res
		}
		select {
		case <-ctx.Done():
			res.TerminalStatus = "canceled"
			res.FailureReason = "campaign canceled"
			res.TerminalAt = time.Now().UTC()
			res.ProvisionPhases = phaseList(phases)
			return res
		case <-time.After(pollEvery):
		}
		st, code, err := client.GetDeployment(ctx, created.DeploymentID)
		if isAuthFailure(code) {
			res.TerminalStatus = "failed"
			res.FailureReason = "authentication failed (token expired or forbidden)"
			res.TerminalAt = time.Now().UTC()
			res.ProvisionPhases = phaseList(phases)
			return res
		}
		if err != nil {
			if code == http.StatusNotFound {
				res.TerminalStatus = "terminated"
				res.FailureReason = "deployment disappeared while provisioning"
				res.TerminalAt = time.Now().UTC()
				res.ProvisionPhases = phaseList(phases)
				return res
			}
			continue
		}
		if phase := stringFromAny(st.MetricsSummary, "provision_phase"); phase != "" {
			phases[phase] = struct{}{}
		}
		switch st.Status {
		case "running":
			res.TerminalStatus = "running"
			res.TerminalAt = time.Now().UTC()
			res.TimeToRunningMS = res.TerminalAt.Sub(res.CreatedAt).Milliseconds()
			res.ProvisionPhases = phaseList(phases)
			return res
		case "failed":
			res.TerminalStatus = "failed"
			res.FailureReason = failureReason(st.MetricsSummary)
			res.TerminalAt = time.Now().UTC()
			res.ProvisionPhases = phaseList(phases)
			return res
		case "terminated":
			res.TerminalStatus = "terminated"
			res.FailureReason = "terminated before running"
			res.TerminalAt = time.Now().UTC()
			res.ProvisionPhases = phaseList(phases)
			return res
		}
	}
}

func isAuthFailure(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

func cleanupTracked(ctx context.Context, client *apiClient, tracker *deploymentTracker) {
	items := tracker.snapshot()
	if len(items) == 0 {
		return
	}
	log.Printf("cleaning up %d deployments", len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.DeleteDeployment(ctx, item.ID); err != nil {
				log.Printf("delete %s: %v", item.ID, err)
			}
		}()
	}
	wg.Wait()

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		remaining := 0
		for _, item := range items {
			st, code, err := client.GetDeployment(ctx, item.ID)
			if err != nil && code == http.StatusNotFound {
				continue
			}
			if err == nil && (st.Status == "terminated" || st.Status == "failed") {
				continue
			}
			if err == nil && code == http.StatusOK && st.Status != "terminated" {
				remaining++
			}
		}
		if remaining == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
	log.Printf("cleanup wait timed out; some namespaces may still be terminating")
}

func phaseList(phases map[string]struct{}) []string {
	out := make([]string, 0, len(phases))
	for p := range phases {
		out = append(out, p)
	}
	return out
}

func stringFromAny(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return fmt.Sprint(v)
	}
	return s
}

func failureReason(summary map[string]any) string {
	if summary == nil {
		return "failed"
	}
	if s := stringFromAny(summary, "error"); s != "" {
		return s
	}
	if diag, ok := summary["diagnostics"].(map[string]any); ok {
		if s := stringFromAny(diag, "outcome"); s != "" && s != "success" {
			return s
		}
	}
	if phase := stringFromAny(summary, "provision_phase"); phase == "failed" {
		return "provision_phase_failed"
	}
	return "failed"
}

func dryRunWave(wave Wave, baseDir, tier string) (WaveReport, error) {
	plans, total, err := planWave(wave, baseDir, tier)
	if err != nil {
		return WaveReport{}, err
	}
	results := make([]DeploymentResult, 0, len(plans))
	for _, p := range plans {
		results = append(results, DeploymentResult{
			Index:        p.index,
			Design:       p.design,
			Weight:       p.weight,
			CreateStatus: "dry-run",
			CreatedAt:    time.Now().UTC(),
		})
	}
	rep := summarizeWave(wave.Name, wave.effectiveMode(), results, nil)
	rep.RequestedWeight = total
	rep.AdmittedCount = 0
	rep.SuccessCount = 0
	rep.SuccessRate = 0
	return rep, nil
}

func ensureMeAllows(ctx context.Context, client *apiClient, needed int) error {
	me, err := client.GetMe(ctx)
	if err != nil {
		return err
	}
	if me.Limits.MaxConcurrent < needed {
		return fmt.Errorf("account maxConcurrent=%d but largest wave requests %d; raise the internal cap or shrink the wave", me.Limits.MaxConcurrent, needed)
	}
	log.Printf("authenticated tier=%s maxConcurrent=%d", me.Tier, me.Limits.MaxConcurrent)
	return nil
}

func largestWaveCount(m Manifest) int {
	max := 0
	for _, w := range m.Waves {
		if n := w.totalCount(); n > max {
			max = n
		}
	}
	return max
}

func printDryRunSummary(rep CampaignReport) {
	fmt.Printf("dry-run campaign %q (tier=%s)\n", rep.Name, rep.Tier)
	fmt.Printf("builtin designs: %s\n", strings.Join(listBuiltinDesigns(), ", "))
	for _, w := range rep.Waves {
		fmt.Printf("  wave %-24s count=%-3d cpu=%dm mem=%dMi mode=%s\n",
			w.Name, w.RequestedCount, w.RequestedWeight.CPUMillicores, w.RequestedWeight.MemoryMi, w.Mode)
	}
}
