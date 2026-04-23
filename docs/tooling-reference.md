# Cloud9 — Developer Tooling Reference
> Reference for every CLI tool used in Cloud9 development.
> Covers: purpose, prerequisite setup, common commands, and troubleshooting.
> Keep this updated when new tools or patterns are introduced.

---

## Table of Contents
1. [Google Cloud SDK (`gcloud`)](#1-google-cloud-sdk-gcloud)
2. [Terraform](#2-terraform)
3. [kubectl](#3-kubectl)
4. [Helm](#4-helm)
5. [Prometheus & Grafana](#5-prometheus--grafana)
6. [Port Forwarding](#6-port-forwarding)
7. [Docker](#7-docker)
8. [Go Toolchain](#8-go-toolchain)
9. [wscat (WebSocket testing)](#9-wscat-websocket-testing)
10. [Local Development Checklist](#10-local-development-checklist)
11. [GitHub Actions WIF Deploy Setup](#11-github-actions-wif-deploy-setup)

---

## 1. Google Cloud SDK (`gcloud`)

### Purpose
`gcloud` is the CLI for Google Cloud Platform. In Cloud9 it is used to:
- Manage the GCP project and billing
- Create and manage IAM service accounts and credentials
- Connect `kubectl` to the GKE Autopilot cluster
- Manage Artifact Registry (push/pull Docker images)

### Prerequisites
```bash
# Install
# https://cloud.google.com/sdk/docs/install

# Verify
gcloud version

# Install required components
gcloud components install gke-gcloud-auth-plugin
gcloud components install kubectl

# Authenticate
gcloud auth login
gcloud config set project ganymede-cloud
```

> **Note:** The GCP project ID is `ganymede-cloud` — this is intentional and must not be changed.
> Quota approvals are tied to this project ID.

### Common commands

#### Project and billing
```bash
# Set active project
gcloud config set project ganymede-cloud

# List billing accounts
gcloud billing accounts list

# Link billing to project
gcloud billing projects link ganymede-cloud --billing-account=BILLING_ACCOUNT_ID
```

#### APIs
```bash
# Enable required APIs (run once on project setup)
gcloud services enable container.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# List enabled APIs
gcloud services list --enabled
```

#### IAM and service accounts
```bash
# List service accounts
gcloud iam service-accounts list

# Create service account
gcloud iam service-accounts create cloud9-terraform \
  --display-name="Cloud9 Terraform"

# Grant role to service account
gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com" \
  --role="roles/container.admin"

# Download credentials key (store at repo root, never commit)
gcloud iam service-accounts keys create terraform-key.json \
  --iam-account=cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com
```

#### Artifact Registry
```bash
# Create Docker repository (run once)
gcloud artifacts repositories create cloud9-images \
  --repository-format=docker \
  --location=us-central1 \
  --description="Cloud9 template images"

# Configure Docker auth (run once per machine)
gcloud auth configure-docker us-central1-docker.pkg.dev

# List images in registry
gcloud artifacts docker images list \
  us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images

# Grant GKE node SA pull access
gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:<compute-sa-email>" \
  --role="roles/artifactregistry.reader"
```

#### GCS (seed data)
```bash
# List bucket contents
gcloud storage ls gs://cloud9-seed-data/

# List specific prefix
gcloud storage ls gs://cloud9-seed-data/url-shortener/
```

#### Quota management
```bash
# Check region quota
gcloud compute regions describe us-central1 \
  --format="table(quotas.metric,quotas.limit,quotas.usage)"

# Quota increases: GCP Console → IAM & Admin → Quotas
# Free trial: max 12 CPUs (all regions). Upgrade account to request more.
```

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `gke-gcloud-auth-plugin not found` | Plugin not installed | `gcloud components install gke-gcloud-auth-plugin` |
| `403 Permission denied` | Wrong active project | `gcloud config set project ganymede-cloud` |
| `quota exceeded` | Free trial CPU limit hit | Upgrade account or request quota increase |
| `gcloud artifacts` not found | Old SDK version | `gcloud components update` |

---

## 2. Terraform

### Purpose
Terraform provisions and manages all GCP infrastructure as code. In Cloud9:
- Creates the GKE Autopilot cluster
- Creates the GCS bucket for seed data dumps
- All infrastructure is reproducible from `terraform apply`

### Prerequisites
```bash
# Install: https://developer.hashicorp.com/terraform/install
terraform version  # verify

# Required files at repo root:
# terraform-key.json  — GCP service account credentials (NEVER commit)
```

### Directory structure
```
infra/
├── main.tf        # GKE cluster + GCS bucket resources
├── variables.tf   # project_id, region, cluster_name etc
├── outputs.tf     # cluster_name, kubeconfig_command, seed_bucket_name
└── versions.tf    # provider version pins
```

### Common commands
```bash
cd infra

# Download providers (run once, or after versions.tf changes)
terraform init

# Preview what will be created/changed/destroyed
terraform plan

# Apply changes (creates real GCP resources)
terraform apply
# Type 'yes' when prompted — GKE takes 7-10 minutes

# Destroy all managed resources
terraform destroy
# Type 'yes' when prompted — takes ~5 minutes

# Show current state
terraform show

# List managed resources
terraform state list
```

### Rebuild from scratch
```bash
cd infra && terraform apply
gcloud container clusters get-credentials cloud9 \
  --region us-central1 --project ganymede-cloud
kubectl config use-context gke_ganymede-cloud_us-central1_cloud9
```

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `403 compute.instanceGroupManagers.get` | SA missing compute.viewer role | Add `roles/compute.viewer` to cloud9-terraform SA |
| `terraform apply` destroys and recreates cluster | State mismatch | Add compute.viewer before first apply |
| `Error: services "kube-prometheus-stack-coredns" is forbidden` | kube-system is managed on Autopilot | Add `coreDns.enabled: false` to Helm values |
| `*.tfstate` committed accidentally | gitignore miss | Add to .gitignore, run `git rm --cached *.tfstate` |

### Important notes
- `terraform.tfstate` maps resource definitions to real GCP resource IDs — never delete it without `terraform destroy` first
- `terraform-key.json` must be in `.gitignore` — contains credentials
- `.terraform/` directory contains downloaded providers — also gitignored
- `.terraform.lock.hcl` **should** be committed — pins provider versions like `go.sum`

---

## 3. kubectl

### Purpose
`kubectl` is the CLI for interacting with Kubernetes clusters. In Cloud9 it is used to:
- Inspect deployment namespaces and pod status
- View pod logs for debugging
- Port-forward services for local testing
- Verify that resources were created correctly by the deployer

### Prerequisites
```bash
# Bundled with Docker Desktop, or:
gcloud components install kubectl

kubectl version --client  # verify

# Connect to GKE cluster
gcloud container clusters get-credentials cloud9 \
  --region us-central1 --project ganymede-cloud
```

### Contexts (switching between clusters)
```bash
# List all contexts
kubectl config get-contexts

# Switch to GKE (production)
kubectl config use-context gke_ganymede-cloud_us-central1_cloud9

# Switch to minikube (local dev)
kubectl config use-context minikube

# Show current context
kubectl config current-context
```

> Always default to minikube for local dev to avoid accidental GKE deployments and charges.

### Common commands

#### Namespaces
```bash
# List all namespaces
kubectl get ns

# List Cloud9 deployment namespaces only
kubectl get ns | grep cloud9

# Delete a namespace (cascades all resources inside)
kubectl delete namespace cloud9-url-shortener-<id>

# Clean up all stuck Cloud9 namespaces
kubectl get ns --no-headers | grep cloud9 | awk '{print $1}' | xargs kubectl delete namespace
```

#### Pods
```bash
# List pods in a namespace
kubectl get pods -n cloud9-url-shortener-<id>

# Watch pods in real time
kubectl get pods -n cloud9-url-shortener-<id> -w

# Describe pod (shows events, resource requests, scheduling issues)
kubectl describe pod -n <namespace> <pod-name>

# Get pod logs
kubectl logs -n <namespace> <pod-name>
kubectl logs -n <namespace> deploy/app-server  # by deployment name
kubectl logs -n <namespace> <pod-name> --previous  # previous container instance

# Exec into a pod
kubectl exec -n <namespace> -it <pod-name> -- /bin/sh

# Run a one-off command in a pod
kubectl exec -n <namespace> deploy/postgres -- \
  psql -U syscrack -d syscrack -c "SELECT COUNT(*) FROM urls;"
```

#### Nodes
```bash
# List nodes (Autopilot: empty until first pod scheduled)
kubectl get nodes

# Node resource usage
kubectl describe nodes | grep -A5 "Allocated resources"
```

#### Monitoring namespace
```bash
# Check monitoring stack pods
kubectl get pods -n monitoring

# Watch monitoring pods
kubectl get pods -n monitoring -w

# Describe a stuck monitoring pod
kubectl describe pod -n monitoring <pod-name>
```

#### Secrets and config
```bash
# List Helm release secrets (for debugging stuck Helm installs)
kubectl get secret -n monitoring -l owner=helm,name=kube-prometheus-stack

# Delete stuck Helm release secrets
kubectl delete secret -n monitoring -l owner=helm,name=kube-prometheus-stack
```

### Troubleshooting

| Symptom | Command | What to look for |
|---------|---------|-----------------|
| Pod stuck Pending | `kubectl describe pod -n <ns> <pod>` | Events section — quota exceeded, insufficient CPU/memory |
| Pod CrashLoopBackOff | `kubectl logs -n <ns> <pod>` | Application error on startup |
| Pod ContainerCreating (stuck) | `kubectl describe pod -n <ns> <pod>` | FailedMount — missing secret |
| Namespace stuck Terminating | `kubectl get ns` | Finalizers blocking deletion |

### GKE Autopilot-specific behaviour
- `kubectl get nodes` returns empty before any pods are scheduled — this is normal
- First pod in a new namespace triggers node provisioning (~2-3 minutes cold start)
- Autopilot enforces minimum resource requests: 250m CPU, 512Mi memory per container
- `kube-system` namespace is managed — you cannot patch resources there
- hostPath volumes are restricted — affects node-exporter in kube-prometheus-stack

---

## 4. Helm

### Purpose
Helm is a package manager for Kubernetes. In Cloud9 it is used to deploy `kube-prometheus-stack` — which includes Prometheus, Grafana, and kube-state-metrics.

### Prerequisites
```bash
# Install via Chocolatey (Windows, run as Administrator)
choco install kubernetes-helm

# Or download from https://helm.sh/docs/intro/install/
helm version  # verify

# Add Prometheus community repo (run once)
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

### Common commands
```bash
# Install or upgrade kube-prometheus-stack
helm upgrade --install kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --values k8s/helm/prometheus-values.yaml

# List installed releases
helm list -n monitoring

# Show release status
helm status kube-prometheus-stack -n monitoring

# Uninstall a release
helm uninstall kube-prometheus-stack -n monitoring

# Preview rendered manifests without applying (useful for debugging)
helm template kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --values k8s/helm/prometheus-values.yaml

# Update repo cache
helm repo update
```

### `k8s/helm/prometheus-values.yaml`
Committed to the repo. Key settings:
- `scrapeInterval: "5s"` — faster scraping for live demo feel
- `additionalScrapeConfigs` — enables annotation-based pod discovery
- `coreDns.enabled: false` — required for GKE Autopilot (kube-system is managed)
- `nodeExporter.enabled: false` — hostPath mounts not allowed on Autopilot
- `admissionWebhooks.enabled: false` — webhook job fails on Autopilot
- `grafana.resources` — reduced from defaults to fit on Autopilot nodes

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `services "kube-prometheus-stack-coredns" is forbidden` | `coreDns` not disabled | Add `coreDns.enabled: false` to values |
| `secret "kube-prometheus-stack-admission" not found` | Admission webhook job failed | Add `admissionWebhooks.patch.enabled: false` and `tls.enabled: false` |
| `helm uninstall` fails | Release in broken state | Delete Helm secrets manually: `kubectl delete secret -n monitoring -l owner=helm,name=kube-prometheus-stack` |
| Pods pending after install | Insufficient cluster resources | Reduce resource requests in values file; check node capacity |
| `Error: UPGRADE FAILED` | Previous failed install left partial state | Delete namespace and reinstall |

### Recovery from broken Helm install
```bash
# 1. Delete Helm release secrets
kubectl delete secret -n monitoring -l owner=helm,name=kube-prometheus-stack

# 2. Delete namespace
kubectl delete namespace monitoring

# 3. Reinstall
helm upgrade --install kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --values k8s/helm/prometheus-values.yaml
```

---

## 5. Prometheus & Grafana

### Purpose
- **Prometheus**: time-series metrics database. Scrapes pod `/metrics` endpoints every 5s. The Go service queries it via HTTP API to stream metrics to the frontend.
- **Grafana**: dashboards for the monitoring stack. Developer-facing only — not user-facing. Useful for verifying Prometheus is scraping correctly and building PromQL queries.

### Accessing Prometheus
```powershell
# Port-forward (keep running in a separate terminal)
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090

# Open in browser
# http://localhost:9090
```

### Accessing Grafana
```powershell
# Port-forward
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80

# Open in browser: http://localhost:3000
# Login: admin / cloud9-dev
```

### Prometheus UI navigation
- **Status → Targets**: shows all pods being scraped. App-server pods appear under `kubernetes-pods` pool when annotations are correct.
- **Status → Configuration**: shows full scrape config including `additionalScrapeConfigs`
- **Query**: run PromQL queries directly

### Key PromQL queries
```promql
# HTTP request rate for a specific deployment
rate(http_requests_total{namespace="cloud9-url-shortener-<id>"}[30s])

# p99 latency
histogram_quantile(0.99,
  rate(http_request_duration_seconds_bucket{namespace="cloud9-url-shortener-<id>"}[1m])
)

# Cache hit rate
rate(cache_hits_total{namespace="cloud9-url-shortener-<id>"}[30s])

# All Cloud9 namespaces
rate(http_requests_total{namespace=~"cloud9.*"}[30s])

# Check if app-server is being scraped
up{namespace=~"cloud9.*"}

# CPU usage (where available)
rate(container_cpu_usage_seconds_total{namespace="cloud9-url-shortener-<id>"}[1m])
```

### Annotation-based scraping
App-server pods must have these annotations for Prometheus to discover them:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "8080"
prometheus.io/path: "/metrics"
```
These are set by `K8sDeployer` in `pkg/deployer/k8s.go`.

### Known limitations on GKE Autopilot
- `container_cpu_usage_seconds_total` may return empty — Autopilot manages nodes differently and doesn't expose standard node-level container metrics via the community stack. Use GKE Managed Prometheus for full metrics coverage (future improvement).
- `node-exporter` cannot run on Autopilot — hostPath mounts to `/proc`, `/sys` are blocked.

### Troubleshooting

| Symptom | Check | Fix |
|---------|-------|-----|
| App-server not in Targets | Status → Targets → kubernetes-pods | Verify pod has scrape annotations; check `additionalScrapeConfigs` in values |
| Metrics return no data | Run `up{namespace=~"cloud9.*"}` | If empty, scraping not working — check annotations and Prometheus config |
| `container_cpu_usage_seconds_total` empty | GKE Autopilot limitation | Expected — use application-level metrics instead |
| Grafana login fails | Wrong password | Default is `cloud9-dev` set in values file |

---

## 6. Port Forwarding

### Purpose
`kubectl port-forward` creates a tunnel from your local machine to a service running inside the GKE cluster. Used for:
- Testing app-server endpoints during development
- Accessing the Prometheus UI
- Accessing the Grafana UI
- Connecting the Go orchestrator to Prometheus

### Port assignment convention
To avoid conflicts, use consistent port assignments:

| Service | Local port | Cluster port | Command |
|---------|-----------|--------------|---------|
| Go orchestrator | 8081 | — | `go run cmd/api/main.go 8081` |
| App-server pod | 8082 | 8080 | `kubectl port-forward svc/app-server 8082:8080` |
| Prometheus | 9090 | 9090 | `kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090` |
| Grafana | 3000 | 80 | `kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80` |

> The format is `local_port:cluster_port`. `8082:8080` means local 8082 → pod's 8080.

### Commands
```powershell
# App-server (in its deployment namespace)
kubectl port-forward -n cloud9-url-shortener-<id> svc/app-server 8082:8080

# Prometheus
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090

# Grafana
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80
```

### Important notes
- Each port-forward needs its own terminal — it blocks while running
- Port-forwards die when the terminal closes or the pod restarts
- The Go orchestrator connects to Prometheus via `PROMETHEUS_URL=http://localhost:9090` when running locally — requires the Prometheus port-forward to be active
- When running the orchestrator on 8081, set `PROMETHEUS_URL=http://localhost:9090` in env vars

### Setting `PROMETHEUS_URL` for local development
```powershell
$env:DEPLOYER="k8s"
$env:SUPABASE_URL="https://<project-ref>.supabase.co"
$env:SUPABASE_SERVICE_KEY="<service-role-key>"
$env:GOOGLE_APPLICATION_CREDENTIALS="terraform-key.json"
$env:PROMETHEUS_URL="http://localhost:9090"
go run cmd/api/main.go 8081
```

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `error: error from server (NotFound)` | Wrong namespace or service name | `kubectl get svc -n <namespace>` to verify service name |
| Port-forward dies unexpectedly | Pod restarted | Restart the port-forward command |
| `bind: address already in use` | Local port already taken | Use a different local port or kill the process using it |
| WebSocket returns 404 via port-forward | Port-forward routing to wrong service | Connect WebSocket to orchestrator port (8081), not pod port-forward |

---

## 7. Docker

### Purpose
Docker builds and pushes the template images (url-shortener, caching-proxy) to GCP Artifact Registry. Images are pulled by GKE at deploy time.

### Prerequisites
- Docker Desktop installed and running
- `gcloud auth configure-docker us-central1-docker.pkg.dev` run once

### Building and pushing images
```bash
# URL shortener (from images/url-shortener/)
docker build -t us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images/url-shortener:v1 .
docker push us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images/url-shortener:v1

# Caching proxy (from images/caching-proxy/)
docker build -t us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images/caching-proxy:v1 .
docker push us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images/caching-proxy:v1
```

### Local Postgres container (seed data generation)
```bash
# Create (first time only)
docker run --name cloud9-local \
  -e POSTGRES_USER=syscrack \
  -e POSTGRES_PASSWORD=syscrack \
  -e POSTGRES_DB=syscrack \
  -p 5432:5432 \
  -d postgres:15

# Start (subsequent sessions)
docker start cloud9-local

# Stop (data preserved)
docker stop cloud9-local

# Verify connection
docker exec -it cloud9-local psql -U syscrack -d syscrack -c "SELECT 1;"
```

### ⚠️ pg_dump version mismatch
Always use `pg_dump` from inside the `cloud9-local` container (postgres:15), not from WSL (which may have a newer PostgreSQL version). A dump created with PG17 cannot be restored by the PG15 pod — `pg_restore` will fail with `unsupported version in file header`.

The seed script (`scripts/seed/generate_urls.py`) handles this automatically by running `pg_dump` via `docker exec cloud9-local`.

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Cannot connect to the Docker daemon` | Docker Desktop not running | Start Docker Desktop |
| `go.mod requires go >= 1.24.0` during build | Dockerfile uses old Go base image | Change `FROM golang:1.22-alpine` to `FROM golang:1.24-alpine` |
| `unauthorized` when pushing | Docker auth not configured | Run `gcloud auth configure-docker us-central1-docker.pkg.dev` |
| `ImagePullBackOff` on GKE | GKE node SA missing pull permissions | Grant `roles/artifactregistry.reader` to compute SA |

---

## 8. Go Toolchain

### Purpose
Builds and runs the Cloud9 orchestrator (`cmd/api/main.go`) and the template images (`images/url-shortener/`, `images/caching-proxy/`).

### Common commands
```bash
# Run orchestrator (default port 8080)
go run cmd/api/main.go

# Run orchestrator on custom port (avoids conflict with port-forward)
go run cmd/api/main.go 8081

# Build all packages
go build ./...

# Run all tests
go test ./...

# Run specific package tests
go test ./pkg/deployer/...
go test ./pkg/teardown/...

# Run with verbose output
go test -v ./pkg/deployer/...

# Add dependency
go get github.com/some/package

# Clean up go.mod and go.sum
go mod tidy
```

### Module structure
Cloud9 uses multiple Go modules:
- `github.com/ywacch/cloud9` — root module (orchestrator)
- `github.com/ywacch/cloud9/images/url-shortener` — separate module
- `github.com/ywacch/cloud9/images/caching-proxy` — separate module

Run `go mod tidy` in each module directory separately after adding dependencies.

### Environment variables for local development
```powershell
$env:DEPLOYER="k8s"                          # use K8sDeployer (targets GKE)
$env:SUPABASE_URL="https://<ref>.supabase.co"
$env:SUPABASE_SERVICE_KEY="<service-role-key>"
$env:GOOGLE_APPLICATION_CREDENTIALS="terraform-key.json"
$env:TEMPLATE_IMAGE_REGISTRY="us-central1-docker.pkg.dev/ganymede-cloud/cloud9-images"
$env:SEED_BUCKET_NAME="cloud9-seed-data"
$env:PROMETHEUS_URL="http://localhost:9090"   # requires Prometheus port-forward
```

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `missing go.sum entry` | Dependency added without tidy | `go get <package>` then `go mod tidy` |
| Race detector fails on Windows | CGO required, not installed | Run race detector in Linux CI instead |
| Build fails after adding package | Wrong module directory | `go mod tidy` in the correct module directory |

---

## 9. wscat (WebSocket testing)

### Purpose
`wscat` is a CLI WebSocket client used to test the metrics streaming endpoint (`GET /deployments/{id}/metrics`) during development.

### Prerequisites
```bash
npm install -g wscat
wscat --version  # verify
```

### Usage
```bash
# Connect to metrics WebSocket (orchestrator must be running)
wscat -c ws://localhost:8081/deployments/cloud9-url-shortener-<id>/metrics

# Expected: JSON payloads every 2 seconds
# Press Ctrl+C to disconnect
```

### What to expect
```json
{
  "timestamp": "2026-03-22T22:56:09Z",
  "namespace": "cloud9-url-shortener-<id>",
  "components": {
    "app-server": {
      "component": "app-server",
      "requestRate": 0.149,
      "p99Latency": 0.0097,
      "cacheHits": 0.104,
      "cacheMisses": 0.035,
      "cacheHitsLast9m": 59,
      "cacheMissesLast9m": 21,
      "errorRate": 0
    },
    "cache": { ... },
    "db": { ... }
  }
}
```

### Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Unexpected server response: 404` | Wrong port — hitting pod port-forward not orchestrator | Use orchestrator port (8081), not pod port-forward port |
| `Connection refused` | Orchestrator not running | Start `go run cmd/api/main.go 8081` |
| All values are 0 | No traffic or Prometheus not scraping yet | Hit some endpoints via port-forward, wait one scrape interval (5s) |
| `error: websocket: the client is not using the websocket protocol` | Plain HTTP request to WebSocket endpoint | Use `wscat` or Postman WebSocket, not `Invoke-RestMethod` |

---

## 10. Local Development Checklist

Run through this before starting a development session.

### Start of session
```powershell
# 1. Verify kubectl context is correct
kubectl config current-context
# Should be: gke_ganymede-cloud_us-central1_cloud9 for GKE
# Or: minikube for local dev

# 2. Check that monitoring stack is running
kubectl get pods -n monitoring
# Expected: grafana, kube-state-metrics, operator, prometheus all Running

# 3. Start Prometheus port-forward (Terminal 1)
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090

# 4. Start orchestrator (Terminal 2)
$env:DEPLOYER="k8s"
$env:SUPABASE_URL="https://<ref>.supabase.co"
$env:SUPABASE_SERVICE_KEY="<service-role-key>"
$env:GOOGLE_APPLICATION_CREDENTIALS="terraform-key.json"
$env:PROMETHEUS_URL="http://localhost:9090"
go run cmd/api/main.go 8081

# 5. POST a deployment (Terminal 3)
Invoke-RestMethod -Uri "http://localhost:8081/deployments" -Method Post `
  -ContentType "application/json" `
  -Body '{"version":"1.0","template":"url-shortener","archetype":"browse-heavy","nodes":[{"id":"n1","type":"db","name":"PostgreSQL","config":{}},{"id":"n2","type":"cache","name":"Redis","config":{"strategy":"cache-aside"}},{"id":"n3","type":"app-server","name":"API Server","config":{}}],"connections":[{"sourceId":"n3","targetId":"n1","protocol":"TCP"},{"sourceId":"n3","targetId":"n2","protocol":"TCP"}]}'

# 6. Watch pods come up
kubectl get pods -n <returned-namespace> -w

# 7. Port-forward app-server (Terminal 4)
kubectl port-forward -n <namespace> svc/app-server 8082:8080

# 8. Test WebSocket stream (Terminal 5)
wscat -c ws://localhost:8081/deployments/<namespace>/metrics
```

### End of session
```powershell
# Clean up any active deployments
kubectl get ns | Select-String cloud9
kubectl delete namespace cloud9-url-shortener-<id>

# Optionally stop port-forwards (Ctrl+C in each terminal)

# Note: leave monitoring stack running — idle cost is minimal
# Only run terraform destroy if you want to fully deprovision
```

### Cost awareness
- GKE Autopilot charges per pod CPU/memory — only when pods are running
- Monitoring stack (Prometheus + Grafana) runs 24/7 — ~$0.30/day
- Each 9-minute deployment costs ~$0.05-0.08
- `terraform destroy` stops all charges but takes 5 minutes to reprovision
- $300 free credits cover the entire project at this usage level

---

## 11. GitHub Actions WIF Deploy Setup

### Purpose
Configure keyless GitHub Actions authentication to GCP via Workload Identity Federation (WIF), then deploy `cloud9-api` to GKE from CI.

### Why this setup
- Avoids storing long-lived GCP JSON keys in GitHub
- Restricts trust to one repo (`Ywacch/runcloud9`)
- Powers `deploy.yaml` secrets:
  - `GCP_WORKLOAD_IDENTITY_PROVIDER`
  - `GCP_SERVICE_ACCOUNT`

### One-time setup commands
```bash
# Ensure project context
gcloud config set project ganymede-cloud

# 1) Create deploy service account
gcloud iam service-accounts create cloud9-github-deployer \
  --display-name="Cloud9 GitHub Deployer"

# 2) Create GitHub WIF pool (separate from GKE internal pool)
gcloud iam workload-identity-pools create github-pool \
  --project ganymede-cloud \
  --location global \
  --display-name "GitHub Actions Pool"

# 3) Create GitHub OIDC provider scoped to this repo
gcloud iam workload-identity-pools providers create-oidc github \
  --project ganymede-cloud \
  --location global \
  --workload-identity-pool github-pool \
  --display-name "GitHub OIDC Provider" \
  --issuer-uri "https://token.actions.githubusercontent.com" \
  --attribute-mapping "google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref,attribute.actor=assertion.actor" \
  --attribute-condition "assertion.repository=='Ywacch/runcloud9'"
```

### Allow repo to impersonate deploy SA
```bash
gcloud iam service-accounts add-iam-policy-binding \
  cloud9-github-deployer@ganymede-cloud.iam.gserviceaccount.com \
  --project ganymede-cloud \
  --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/projects/605217578213/locations/global/workloadIdentityPools/github-pool/attribute.repository/Ywacch/runcloud9"
```

### GitHub Actions secret values
```bash
# Value for GCP_WORKLOAD_IDENTITY_PROVIDER
gcloud iam workload-identity-pools providers describe github \
  --project ganymede-cloud \
  --location global \
  --workload-identity-pool github-pool \
  --format="value(name)"
```

- `GCP_WORKLOAD_IDENTITY_PROVIDER`: output of command above
- `GCP_SERVICE_ACCOUNT`: `cloud9-github-deployer@ganymede-cloud.iam.gserviceaccount.com`

### Where to store in GitHub
Repository -> **Settings** -> **Secrets and variables** -> **Actions** -> **Repository secrets**

Create:
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT`

### Quick diagnostics
```bash
# List pools (you will also see the GKE internal pool)
gcloud iam workload-identity-pools list --project ganymede-cloud --location global

# List providers in github-pool
gcloud iam workload-identity-pools providers list \
  --project ganymede-cloud \
  --location global \
  --workload-identity-pool github-pool

# List service accounts
gcloud iam service-accounts list --project ganymede-cloud
```

### Notes
- `ganymede-cloud.svc.id.goog` is the GKE internal pool. Keep it; it is not the GitHub Actions provider.
- Runtime backend env vars (`SUPABASE_SERVICE_KEY`, `SUPABASE_JWT_SECRET`, etc.) belong in Kubernetes secret `cloud9-api-secrets`, not in GitHub Actions secrets.
