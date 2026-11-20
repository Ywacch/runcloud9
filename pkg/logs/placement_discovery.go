package logs

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/placement"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func discoverPodLogStreams(ctx context.Context, clientset kubernetes.Interface, namespace string, topo *placement.Topology) []podLogStream {
	if topo != nil && len(topo.Nodes) > 0 {
		return discoverPlacementPodLogStreams(ctx, clientset, namespace, *topo)
	}
	return discoverLegacyPodLogStreams(ctx, clientset, namespace)
}

func discoverLegacyPodLogStreams(ctx context.Context, clientset kubernetes.Interface, namespace string) []podLogStream {
	streams := make([]podLogStream, 0, len(componentLogTargets))
	seen := make(map[string]struct{})
	for component, targets := range componentLogTargets {
		for _, target := range targets {
			pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: target.labelSelector,
			})
			if err != nil || len(pods.Items) == 0 {
				continue
			}
			for _, pod := range pods.Items {
				stream := podLogStream{
					component:     component,
					podName:       pod.Name,
					containerName: target.containerName,
				}
				key := stream.key()
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				streams = append(streams, stream)
			}
		}
	}
	return streams
}

func discoverPlacementPodLogStreams(ctx context.Context, clientset kubernetes.Interface, namespace string, topo placement.Topology) []podLogStream {
	streams := make([]podLogStream, 0, 16)
	seen := make(map[string]struct{})
	for _, node := range topo.Nodes {
		for _, member := range node.Members {
			label := fmt.Sprintf("app=%s", member.AppLabel)
			pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
			if err != nil || len(pods.Items) == 0 {
				continue
			}
			container := member.Container
			if container == "" {
				container = member.AppLabel
			}
			comp := logComponentForMember(node, member, topo)
			for _, pod := range pods.Items {
				stream := podLogStream{
					component:     comp,
					nodeID:        node.NodeID,
					podName:       pod.Name,
					containerName: container,
				}
				key := stream.key()
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				streams = append(streams, stream)
			}
		}
	}
	if len(streams) == 0 {
		return discoverLegacyPodLogStreams(ctx, clientset, namespace)
	}
	return streams
}

func logComponentForMember(node placement.Node, member placement.Member, topo placement.Topology) string {
	if member.AppLabel == "work-queue" {
		return "mq"
	}
	comps := topo.ComponentsOnNode(node.NodeID)
	if len(comps) > 0 {
		return comps[0]
	}
	return legacyComponentFromNodeType(node.NodeType)
}

func legacyComponentFromNodeType(nodeType string) string {
	switch nodeType {
	case "app-server":
		return "app-server"
	case "worker":
		return "worker"
	case "lb":
		return "lb"
	case "db":
		return "db"
	case "mq":
		return "mq"
	case "cache":
		return "cache"
	default:
		return nodeType
	}
}
