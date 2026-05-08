# Cloud9 — GCP & Terraform Setup Guide
> Run these steps once to provision the GKE Autopilot cluster and GCS bucket.  
> Keep this doc updated if infrastructure changes.

This guide is GCP-specific. For provider-agnostic self-host steps and canonical runtime config, see:

- `docs/self-host-deployment.md`
- `docs/env-and-secrets-reference.md`

> **Note:** The GCP project ID remains `ganymede-cloud` — this is intentional.  
> Existing quota approvals are tied to this project ID and must not be changed.

---

## Prerequisites

| Tool | Install | Verify |
|------|---------|--------|
| Google Cloud SDK | https://cloud.google.com/sdk/docs/install | `gcloud version` |
| gke-gcloud-auth-plugin | `gcloud components install gke-gcloud-auth-plugin` | `gke-gcloud-auth-plugin --version` |
| Terraform | https://developer.hashicorp.com/terraform/install | `terraform version` |
| kubectl | Bundled with Docker Desktop or `gcloud components install kubectl` | `kubectl version --client` |

---

## 1. GCP Project Setup

```bash
# Create project. ganymede-cloud is the old project name where quotas
# we increased. If you wish to use an entirely different project use another name
gcloud projects create ganymede-cloud --name="Cloud9"

# Set as active project
gcloud config set project ganymede-cloud

# Optional: update display name only
gcloud projects update ganymede-cloud --name="Cloud9"

# Link billing account — list accounts first
gcloud billing accounts list

# Link billing (replace BILLING_ACCOUNT_ID)
gcloud billing projects link ganymede-cloud --billing-account=BILLING_ACCOUNT_ID

# Enable required APIs
gcloud services enable container.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Verify APIs enabled
gcloud services list --enabled | Select-String -Pattern "container|storage|iam|artifactregistry"
```

---

## 2. Terraform Service Account

```bash
# Create service account
gcloud iam service-accounts create cloud9-terraform \
  --display-name="Cloud9 Terraform"

# Grant roles
gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com" \
  --role="roles/container.admin"

gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

# Required for Terraform to read back cluster state after creation
gcloud projects add-iam-policy-binding ganymede-cloud \
  --member="serviceAccount:cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com" \
  --role="roles/compute.viewer"

# Download credentials key to repo root
gcloud iam service-accounts keys create terraform-key.json \
  --iam-account=cloud9-terraform@ganymede-cloud.iam.gserviceaccount.com
```

> ⚠️ `terraform-key.json` must never be committed to git.  
> Verify `.gitignore` contains: `terraform-key.json`, `*.tfstate`, `*.tfstate.backup`, `.terraform/`

---

## 3. Provision Infrastructure with Terraform

```bash
# From repo root
cd infra

# Download GCP provider
terraform init

# Preview changes
terraform plan

# Apply — takes 7-10 minutes for GKE Autopilot
terraform apply
# Type 'yes' when prompted
```

Expected outputs after apply:
```
cluster_location    = "us-central1"
cluster_name        = "cloud9"
kubeconfig_command  = "gcloud container clusters get-credentials cloud9 --region us-central1 --project ganymede-cloud"
seed_bucket_name    = "cloud9-seed-data"
```

---

## 4. Connect kubectl to GKE

```bash
# Configure kubectl to use GKE cluster
gcloud container clusters get-credentials cloud9 --region us-central1 --project ganymede-cloud

# Verify context
kubectl config current-context
# Expected: gke_ganymede-cloud_us-central1_cloud9

# Verify cluster (Autopilot shows no nodes until pods are scheduled — that's normal)
kubectl get nodes
kubectl get ns
```

---

## 5. Switching Between Contexts

```bash
# List all contexts (minikube + GKE)
kubectl config get-contexts

# Switch to GKE
kubectl config use-context gke_ganymede-cloud_us-central1_cloud9

# Switch back to minikube (local dev)
kubectl config use-context minikube
```

> Default to minikube for local dev to avoid accidental GKE deployments and charges.

---

## 6. Running the API Against GKE

```powershell
# Windows PowerShell
$env:DEPLOYER="k8s"
$env:SUPABASE_URL="https://<project-ref>.supabase.co"
$env:SUPABASE_SERVICE_KEY="<service-role-key>"
go run cmd/api/main.go
```

Ensure kubectl context is set to GKE before starting the API.

When deploying API manifests to GKE, apply base plus GKE overlay:

```bash
kubectl apply -k k8s/base
kubectl apply -k k8s/overlays/gke
kubectl apply -f k8s/base/cloud9-api-secrets.example.yaml
```

---

## 7. Tearing Down Infrastructure

```bash
# Destroy all Terraform-managed resources (cluster + GCS bucket)
cd infra
terraform destroy
# Type 'yes' when prompted — takes ~5 minutes
```

> ⚠️ Do this when not actively developing to avoid GCP charges.  
> Your $300 free credits cover significant usage but don't leave the cluster running indefinitely.

---

## 8. Rebuilding From Scratch

If you need to recreate everything (e.g. after `terraform destroy`):

```bash
cd infra && terraform apply          # reprovision cluster + bucket
gcloud container clusters get-credentials cloud9 --region us-central1 --project ganymede-cloud
kubectl config use-context gke_ganymede-cloud_us-central1_cloud9
```

That's it. Infrastructure is fully reproducible.

---

## Known Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| `gke-gcloud-auth-plugin not found` | Plugin not installed | `gcloud components install gke-gcloud-auth-plugin` |
| `403 compute.instanceGroupManagers.get` | Service account missing compute.viewer role | Run step 2 compute.viewer binding |
| `terraform apply` destroys and recreates cluster | State mismatch after 403 error | Add compute.viewer role before first apply |
| `kubectl get nodes` shows nothing | Autopilot — nodes provision on first pod schedule | Expected behavior, not a bug |
