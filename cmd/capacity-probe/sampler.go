package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ClusterSample is one optional Kubernetes pressure snapshot.
type ClusterSample struct {
	At               time.Time `json:"at"`
	NodeCount        int       `json:"node_count"`
	AllocatableCPU_m int64     `json:"allocatable_cpu_m"`
	AllocatableMemMi int64     `json:"allocatable_memory_mi"`
	PodsPending      int       `json:"pods_pending"`
	PodsRunning      int       `json:"pods_running"`
	PodsReady        int       `json:"pods_ready"`
	Restarts         int       `json:"restarts"`
	Unschedulable    int       `json:"unschedulable_pods"`
	QuotaEvents      int       `json:"quota_events"`
	ImagePullEvents  int       `json:"image_pull_events"`
	FailedEvents     int       `json:"failed_events"`
}

type clusterSampler struct {
	client *kubernetes.Clientset
	every  time.Duration

	mu      sync.Mutex
	samples []ClusterSample
}

func newClusterSampler(kubeconfig string, every time.Duration) (*clusterSampler, error) {
	cfg, err := loadKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	if every <= 0 {
		every = 2 * time.Second
	}
	return &clusterSampler{client: client, every: every}, nil
}

func loadKubeConfig(explicit string) (*rest.Config, error) {
	if explicit != "" {
		return clientcmd.BuildConfigFromFlags("", explicit)
	}
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return clientcmd.BuildConfigFromFlags("", env)
	}
	home, err := os.UserHomeDir()
	if err == nil {
		fallback := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(fallback); err == nil {
			return clientcmd.BuildConfigFromFlags("", fallback)
		}
	}
	return rest.InClusterConfig()
}

func (s *clusterSampler) run(ctx context.Context) {
	ticker := time.NewTicker(s.every)
	defer ticker.Stop()
	s.capture(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.capture(ctx)
		}
	}
}

func (s *clusterSampler) snapshot() []ClusterSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ClusterSample, len(s.samples))
	copy(out, s.samples)
	return out
}

func (s *clusterSampler) capture(ctx context.Context) {
	sample := ClusterSample{At: time.Now().UTC()}
	nodes, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("capacity-probe sampler: list nodes: %v", err)
	} else {
		sample.NodeCount = len(nodes.Items)
		for _, n := range nodes.Items {
			if cpu, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
				sample.AllocatableCPU_m += cpu.MilliValue()
			}
			if mem, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
				sample.AllocatableMemMi += mem.Value() / (1024 * 1024)
			}
		}
	}

	pods, err := s.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("capacity-probe sampler: list pods: %v", err)
	} else {
		for _, p := range pods.Items {
			if !isCloud9Namespace(p.Namespace) {
				continue
			}
			switch p.Status.Phase {
			case corev1.PodPending:
				sample.PodsPending++
				for _, cond := range p.Status.Conditions {
					if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
						sample.Unschedulable++
					}
				}
			case corev1.PodRunning:
				sample.PodsRunning++
			}
			ready := true
			for _, cs := range p.Status.ContainerStatuses {
				sample.Restarts += int(cs.RestartCount)
				if !cs.Ready {
					ready = false
				}
			}
			if p.Status.Phase == corev1.PodRunning && ready {
				sample.PodsReady++
			}
		}
	}

	events, err := s.client.CoreV1().Events("").List(ctx, metav1.ListOptions{Limit: 500})
	if err != nil {
		log.Printf("capacity-probe sampler: list events: %v", err)
	} else {
		cutoff := time.Now().Add(-30 * time.Second)
		for _, e := range events.Items {
			if !isCloud9Namespace(e.Namespace) {
				continue
			}
			ts := e.LastTimestamp.Time
			if ts.IsZero() {
				ts = e.EventTime.Time
			}
			if ts.Before(cutoff) {
				continue
			}
			msg := e.Reason + " " + e.Message
			switch {
			case containsFold(msg, "exceeded quota"), containsFold(msg, "Forbidden"):
				sample.QuotaEvents++
			case containsFold(msg, "ImagePull"), containsFold(msg, "ErrImagePull"), containsFold(msg, "ImagePullBackOff"):
				sample.ImagePullEvents++
			case e.Type == corev1.EventTypeWarning:
				sample.FailedEvents++
			}
		}
	}

	s.mu.Lock()
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
}

func isCloud9Namespace(ns string) bool {
	return strings.HasPrefix(ns, "cloud9-")
}

func containsFold(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
