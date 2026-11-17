package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		kubeconfig string
		grace      time.Duration
	)
	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path (empty = in-cluster config)")
	flag.DurationVar(&grace, "grace", 5*time.Minute, "grace after cloud9.io/expires-at before delete")
	flag.Parse()

	cfg, err := restConfig(kubeconfig)
	if err != nil {
		log.Fatalf("reaper: kube config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("reaper: client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	deleted, err := Reap(ctx, clientset, Config{Grace: grace, Now: time.Now})
	if err != nil {
		log.Fatalf("reaper: %v", err)
	}
	for _, ns := range deleted {
		fmt.Fprintf(os.Stderr, "reaper: deleted %s\n", ns)
	}
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
