package deployer

import (
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

const imageMemcached = "memcached:1.6"

// memcachedServerArgs sets -m from 90% of the container memory limit, in MiB.
// The floor is 32 MiB so a small container can still allocate slab pages.
func memcachedServerArgs(_ map[string]interface{}, resources corev1.ResourceRequirements) []string {
	const defaultMB = 32
	mem := resources.Limits.Memory()
	mb := int64(defaultMB)
	if mem != nil && !mem.IsZero() {
		mb = mem.Value() * 90 / 100 / (1024 * 1024)
		if mb < defaultMB {
			mb = defaultMB
		}
	}
	// -v logs errors and warnings. -vv would write one line per command.
	return []string{"-m", strconv.FormatInt(mb, 10), "-v"}
}
