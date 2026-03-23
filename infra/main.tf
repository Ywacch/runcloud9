provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("${path.module}/../terraform-key.json")
}

# UNDERSTAND: enable_autopilot = true means GKE manages node provisioning.
# You never create node pools manually. GKE scales nodes based on pod resource requests.
# Cost: pay per pod CPU/memory, not per node VM. Idle cost is near zero.
resource "google_container_cluster" "ganymede" {
  name     = var.cluster_name
  location = var.region

  enable_autopilot = true

  # UNDERSTAND: release_channel REGULAR means GKE auto-upgrades Kubernetes
  # on a regular cadence. STABLE is slower but safer for production.
  # For a portfolio project, REGULAR is fine.
  release_channel {
    channel = "REGULAR"
  }

  # UNDERSTAND: deletion_protection = false allows terraform destroy to delete
  # the cluster. Default is true which prevents accidental deletion.
  # For a dev/portfolio project, set to false so cleanup is easy.
  deletion_protection = false
}

# UNDERSTAND: uniform_bucket_level_access = true means access is controlled
# by IAM only, not per-object ACLs. Simpler and more secure.
resource "google_storage_bucket" "seed_data" {
  name          = var.seed_bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}
