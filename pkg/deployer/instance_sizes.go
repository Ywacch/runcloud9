package deployer

import (
	"fmt"
	"strings"
)

const instanceSizeLabelKey = "cloud9.io/instance-size"

// InstanceSizeGroup is one homogeneous size bucket in first-seen order.
type InstanceSizeGroup struct {
	Size  string
	Count int32
}

func normalizeInstanceSize(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "medium", "large":
		return s
	case "small", "alpha", "":
		return "small"
	default:
		return "small"
	}
}

func instanceSizesFromConfig(config map[string]any, replicaCount int) []string {
	if replicaCount < 1 {
		replicaCount = 1
	}
	def := normalizeInstanceSize(configString(config, "instanceSize", "small"))
	out := make([]string, replicaCount)
	for i := range out {
		out[i] = def
	}
	raw, ok := config["instanceSizes"]
	if !ok || raw == nil {
		return out
	}
	switch arr := raw.(type) {
	case []any:
		for i := 0; i < replicaCount && i < len(arr); i++ {
			out[i] = normalizeInstanceSize(fmt.Sprint(arr[i]))
		}
	case []string:
		for i := 0; i < replicaCount && i < len(arr); i++ {
			out[i] = normalizeInstanceSize(arr[i])
		}
	}
	return out
}

func sizeGroups(sizes []string) []InstanceSizeGroup {
	counts := make(map[string]int32)
	var order []string
	for _, s := range sizes {
		size := normalizeInstanceSize(s)
		if _, seen := counts[size]; !seen {
			order = append(order, size)
		}
		counts[size]++
	}
	groups := make([]InstanceSizeGroup, 0, len(order))
	for _, size := range order {
		groups = append(groups, InstanceSizeGroup{Size: size, Count: counts[size]})
	}
	return groups
}

func sizesUniform(sizes []string) bool {
	if len(sizes) == 0 {
		return true
	}
	first := normalizeInstanceSize(sizes[0])
	for _, s := range sizes[1:] {
		if normalizeInstanceSize(s) != first {
			return false
		}
	}
	return true
}

func nginxWeightForSize(size string) int {
	switch normalizeInstanceSize(size) {
	case "medium":
		return 2
	case "large":
		return 4
	default:
		return 1
	}
}

func weightsAllEqual(weights []int) bool {
	if len(weights) == 0 {
		return true
	}
	first := weights[0]
	for _, w := range weights[1:] {
		if w != first {
			return false
		}
	}
	return true
}
