package api

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"log"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/manifest"
	"github.com/ywacch/cloud9/pkg/templates"
)

func randomManifestRNGSeed() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 1
	}
	n := int64(binary.BigEndian.Uint64(b[:]) & (1<<63 - 1))
	if n <= 0 {
		return 1
	}
	return n
}

func (h *Handler) manifestRNGSeed() int64 {
	if h != nil && h.manifestRNG != nil {
		return h.manifestRNG()
	}
	return randomManifestRNGSeed()
}

func manifestSeedRef(tmpl *templates.TemplateSpec) string {
	if tmpl != nil && tmpl.SeedConfig != nil && tmpl.SeedConfig.DumpPath != "" {
		return tmpl.SeedConfig.DumpPath
	}
	return "fallback:SeedIfEmpty"
}

func manifestImageVersions(templateID string) map[string]string {
	out := map[string]string{}
	if img := templates.GetAppServerImage(templateID); img != "" {
		out["appServer"] = img
	}
	if w := templates.GetWorkerImage(templateID); w != "" {
		out["worker"] = w
	}
	if lg := deployer.LoadgenImage(); lg != "" {
		out["loadgen"] = lg
	}
	return out
}

func (h *Handler) buildRunManifest(design deployer.DeploymentDesign, now time.Time) (manifest.RunManifest, error) {
	return manifest.Build(manifest.Input{
		TemplateID:      design.Template,
		TemplateVersion: templates.TemplateSourceVersion(design.Template),
		ImageVersions:   manifestImageVersions(design.Template),
		SeedRef:         manifestSeedRef(templates.GetTemplate(design.Template)),
		Design:          design,
		RNGSeed:         h.manifestRNGSeed(),
		Now:             now,
	})
}

func (h *Handler) stampRunManifest(ctx context.Context, namespace string, design deployer.DeploymentDesign) {
	if h == nil || h.store == nil || namespace == "" {
		return
	}
	m, err := h.buildRunManifest(design, time.Now().UTC())
	if err != nil {
		log.Printf("run manifest namespace=%s: %v", namespace, err)
		return
	}
	encoded, err := manifest.EncodeSummary(m)
	if err != nil {
		log.Printf("run manifest encode namespace=%s: %v", namespace, err)
		return
	}
	h.stampMetrics(ctx, namespace, map[string]any{
		"run_manifest": encoded,
	})
}

func (h *Handler) stampRunManifestLoadProfile(ctx context.Context, namespace string, profile loadgen.LoadProfile) {
	if h == nil || h.store == nil || namespace == "" {
		return
	}
	row, err := h.store.GetDeployment(ctx, namespace)
	if err != nil {
		log.Printf("run manifest load profile namespace=%s: load row: %v", namespace, err)
		return
	}
	rm, err := manifest.DecodeSummary(row.MetricsSummary["run_manifest"])
	if err != nil {
		log.Printf("run manifest load profile namespace=%s: %v", namespace, err)
		return
	}
	b, err := json.Marshal(profile)
	if err != nil {
		log.Printf("run manifest load profile namespace=%s: marshal: %v", namespace, err)
		return
	}
	raw := json.RawMessage(b)
	if rm.LoadProfile != nil {
		rm.LoadProfileChanges++
	} else {
		rm.LoadProfile = &raw
	}
	encoded, err := manifest.EncodeSummary(rm)
	if err != nil {
		log.Printf("run manifest load profile namespace=%s: encode: %v", namespace, err)
		return
	}
	h.stampMetrics(ctx, namespace, map[string]any{
		"run_manifest": encoded,
	})
}
