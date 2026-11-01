package metrics

import (
	"strconv"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
)

const scaleEventDescription = "CPU autoscaling uses metrics-server samples from about 60 seconds ago."

func scaleEventTitle(component string) string {
	if component == "worker" {
		return "Workers scaled"
	}
	return "App servers scaled"
}

// scaleStatusForApp sums HPA status.desiredReplicas for Deployments that match app.
// Returns nil when no HPA matches or desired is still unreported (0).
func scaleStatusForApp(hpas []autoscalingv2.HorizontalPodAutoscaler, app string, ready int32) *ScaleStatus {
	var desired int32
	var matched bool
	for _, hpa := range hpas {
		ref := hpa.Spec.ScaleTargetRef
		if ref.Kind != "Deployment" {
			continue
		}
		if !deploymentNameMatchesApp(ref.Name, app) {
			continue
		}
		matched = true
		desired += hpa.Status.DesiredReplicas
	}
	if !matched || desired == 0 {
		return nil
	}
	return &ScaleStatus{Desired: desired, Ready: ready}
}

// diffScaleEvents tracks last desired replica counts per component key (app-server, worker).
// The first sample for a key is stored without emitting. A change emits one scale event.
func diffScaleEvents(prev map[string]int32, next map[string]int32, now time.Time) ([]Event, map[string]int32) {
	stored := make(map[string]int32, len(prev))
	for k, v := range prev {
		stored[k] = v
	}
	var events []Event
	for k, newDesired := range next {
		old, ok := stored[k]
		if !ok {
			stored[k] = newDesired
			continue
		}
		if old == newDesired {
			continue
		}
		events = append(events, Event{
			Timestamp:   now,
			Type:        "action",
			Source:      "autoscaler",
			Component:   k,
			Kind:        "scale",
			Visibility:  VisibilityAll,
			Title:       scaleEventTitle(k),
			Description: scaleEventDescription,
			Detail: map[string]string{
				"from":     strconv.FormatInt(int64(old), 10),
				"to":       strconv.FormatInt(int64(newDesired), 10),
				"reason":   "cpu",
				"fidelity": "real",
			},
		})
		stored[k] = newDesired
	}
	for k := range stored {
		if _, ok := next[k]; !ok {
			delete(stored, k)
		}
	}
	return events, stored
}

func (c *MetricsCollector) appendScaleEvents(namespace string, next map[string]int32, now time.Time) []Event {
	if c == nil || next == nil {
		return nil
	}
	c.scaleDesiredMu.Lock()
	defer c.scaleDesiredMu.Unlock()
	if c.scaleDesired == nil {
		c.scaleDesired = make(map[string]map[string]int32)
	}
	prev := c.scaleDesired[namespace]
	events, updated := diffScaleEvents(prev, next, now)
	if len(updated) == 0 {
		delete(c.scaleDesired, namespace)
	} else {
		c.scaleDesired[namespace] = updated
	}
	return events
}
