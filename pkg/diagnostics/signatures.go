package diagnostics

import (
	"regexp"
	"strings"
)

type signatureRule struct {
	code     string
	pattern  *regexp.Regexp
	source   string
	severity string
	message  string
}

var (
	eventSignatureRules = []signatureRule{
		{code: "SCHEDULING_INSUFFICIENT_MEMORY", pattern: regexp.MustCompile(`(?i)FailedScheduling.*Insufficient memory`), source: "event", severity: "error", message: "Pod scheduling blocked by insufficient memory"},
		{code: "SCHEDULING_INSUFFICIENT_CPU", pattern: regexp.MustCompile(`(?i)FailedScheduling.*Insufficient cpu`), source: "event", severity: "error", message: "Pod scheduling blocked by insufficient CPU"},
		{code: "SCHEDULING_UNTOLERATED_TAINT", pattern: regexp.MustCompile(`(?i)FailedScheduling.*untolerated taint`), source: "event", severity: "warning", message: "Pod scheduling blocked by untolerated node taint"},
		{code: "SCHEDULING_QUOTA_EXCEEDED", pattern: regexp.MustCompile(`(?i)quota.*exceeded|exceeded.*quota`), source: "event", severity: "error", message: "Pod scheduling blocked by quota exhaustion"},
		{code: "READINESS_PROBE_FAILED", pattern: regexp.MustCompile(`(?i)Readiness probe failed.*connection refused`), source: "event", severity: "warning", message: "Readiness probe failing with connection refused"},
		{code: "IMAGE_PULL_FAILURE", pattern: regexp.MustCompile(`(?i)ErrImagePull|ImagePullBackOff|ErrImageNeverPull`), source: "event", severity: "error", message: "Container image pull failed"},
		{code: "CONTAINER_BACK_OFF", pattern: regexp.MustCompile(`(?i)Back-off restarting|CrashLoopBackOff`), source: "event", severity: "error", message: "Container restart back-off detected"},
	}
	orchestrationSignatureRules = []signatureRule{
		{code: "WORKER_READY_TIMEOUT", pattern: regexp.MustCompile(`(?i)timed out waiting for worker ready`), source: "orchestration", severity: "error", message: "Worker readiness timeout"},
		{code: "ORCHESTRATION_FAILED", pattern: regexp.MustCompile(`(?i)orchestration failed`), source: "orchestration", severity: "error", message: "Deployment orchestration failed"},
		{code: "SEEDING_FAILED", pattern: regexp.MustCompile(`(?i)seeding failed|pg_restore failed|seed.*timeout`), source: "orchestration", severity: "error", message: "Database seeding failed"},
	}
	logSignatureRules = []signatureRule{
		{code: "LOADGEN_POOL_EMPTY", pattern: regexp.MustCompile(`(?i)loadgen pool empty`), source: "log", severity: "warning", message: "Loadgen pool is empty"},
	}
)

// ExtractSignatures detects known failure signatures from events, orchestration error text, and logs.
func ExtractSignatures(events []EventSnapshot, orchErr string, logs map[string][]string, pods []PodSnapshot) []FailureSignature {
	seen := make(map[string]FailureSignature)

	for _, evt := range events {
		text := strings.TrimSpace(evt.Reason + " " + evt.Message)
		for _, rule := range eventSignatureRules {
			if rule.pattern.MatchString(text) {
				seen[rule.code] = FailureSignature{Code: rule.code, Message: rule.message, Source: rule.source, Severity: rule.severity}
			}
		}
	}

	for _, rule := range orchestrationSignatureRules {
		if rule.pattern.MatchString(orchErr) {
			seen[rule.code] = FailureSignature{Code: rule.code, Message: rule.message, Source: rule.source, Severity: rule.severity}
		}
	}

	for _, lines := range logs {
		for _, line := range lines {
			for _, rule := range logSignatureRules {
				if rule.pattern.MatchString(line) {
					seen[rule.code] = FailureSignature{Code: rule.code, Message: rule.message, Source: rule.source, Severity: rule.severity}
				}
			}
		}
	}

	for _, pod := range pods {
		for _, c := range pod.Containers {
			if strings.Contains(strings.ToLower(c.State), "oomkilled") {
				seen["OOM_KILLED"] = FailureSignature{
					Code:     "OOM_KILLED",
					Message:  "Container terminated with OOMKilled",
					Source:   "pod_status",
					Severity: "error",
				}
			}
		}
	}

	out := make([]FailureSignature, 0, len(seen))
	for _, sig := range seen {
		out = append(out, sig)
	}
	return out
}
