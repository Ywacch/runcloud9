package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ywacch/ganymede/pkg/api"
	"github.com/ywacch/ganymede/pkg/deployer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// buildDeployers returns the deployer map. os.Getenv returns empty string if var not set.
// Default to local so tests and local dev without minikube still work.
func buildDeployers() map[string]deployer.ComponentDeployer {
	if os.Getenv("DEPLOYER") != "k8s" {
		log.Println("Using LocalDeployer (set DEPLOYER=k8s for real K8s)")
		return map[string]deployer.ComponentDeployer{
			"db":         &deployer.LocalDeployer{NodeType: "db"},
			"cache":      &deployer.LocalDeployer{NodeType: "cache"},
			"app-server": &deployer.LocalDeployer{NodeType: "app-server"},
			"lb":         &deployer.LocalDeployer{NodeType: "lb"},
			"mq":         &deployer.LocalDeployer{NodeType: "mq"},
		}
	}

	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("build kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("build clientset: %v", err)
	}

	log.Println("Using K8sDeployer (minikube/GKE)")
	return map[string]deployer.ComponentDeployer{
		"db":         deployer.NewK8sDeployer(clientset, "db"),
		"cache":      deployer.NewK8sDeployer(clientset, "cache"),
		"app-server": &deployer.LocalDeployer{NodeType: "app-server"}, // Week 4
		"lb":         &deployer.LocalDeployer{NodeType: "lb"},
		"mq":         &deployer.LocalDeployer{NodeType: "mq"},
	}
}

func main() {
	deployers := buildDeployers()

	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	handler := api.NewHandler(deployers)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	log.Printf("Ganymede Cloud API starting on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
