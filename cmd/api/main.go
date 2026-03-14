package main

import (
	"log"
	"net/http"
	"os"

	"github.com/ywacch/ganymede/pkg/api"
	"github.com/ywacch/ganymede/pkg/deployer"
)

func main() {
	//  We register one LocalDeployer per component type.
	// In Week 3, these get replaced with K8sDeployer implementations
	// without changing anything in handler.go or orchestrate() — that's
	// the entire point of the ComponentDeployer interface.
	deployers := map[string]deployer.ComponentDeployer{
		"db":         &deployer.LocalDeployer{NodeType: "db"},
		"cache":      &deployer.LocalDeployer{NodeType: "cache"},
		"app-server": &deployer.LocalDeployer{NodeType: "app-server"},
		"lb":         &deployer.LocalDeployer{NodeType: "lb"},
		"mq":         &deployer.LocalDeployer{NodeType: "mq"},
	}

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
