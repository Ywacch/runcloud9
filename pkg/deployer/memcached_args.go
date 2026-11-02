package deployer

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

const imageMemcached = "memcached:1.6"

// memcachedServerArgs sets -m from 90% of the container memory limit (MiB).
func memcachedServerArgs(_ map[string]interface{}, resources corev1.ResourceRequirements) []string {
	const defaultMB = 64
	mem := resources.Limits.Memory()
	if mem == nil || mem.IsZero() {
		return []string{"-m", strconv.Itoa(defaultMB)}
	}
	bytes := mem.Value() * 90 / 100
	mb := bytes / (1024 * 1024)
	if mb < defaultMB {
		mb = defaultMB
	}
	return []string{"-m", strconv.FormatInt(mb, 10)}
}
