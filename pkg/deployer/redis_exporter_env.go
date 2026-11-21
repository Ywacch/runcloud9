package deployer

import (
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
	corev1 "k8s.io/api/core/v1"
)

func redisExporterEnv(cfg DeployConfig) []corev1.EnvVar {
	env := []corev1.EnvVar{{Name: "REDIS_ADDR", Value: "localhost:6379"}}
	tmpl := templates.GetTemplate(cfg.Design.Template)
	if tmpl == nil {
		return env
	}
	bindings := ResolveJobBindings(cfg.Design, tmpl)
	hasWorkQueue := false
	for _, b := range bindings {
		if b.NodeID == cfg.Node.ID && b.Job == "work_queue" && b.Engine == "redis" {
			hasWorkQueue = true
			break
		}
	}
	if !hasWorkQueue {
		return env
	}
	mqSpec, ok := tmpl.Introspection["mq"]
	if !ok || len(mqSpec.Queues) == 0 {
		return env
	}
	env = append(env, corev1.EnvVar{
		Name:  "REDIS_EXPORTER_CHECK_SINGLE_STREAMS",
		Value: strings.Join(mqSpec.Queues, ","),
	})
	return env
}
