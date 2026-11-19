provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("${path.module}/../terraform-key.json")
}

# GKE cluster resources live in gke_standard.tf and network_standard.tf.
# This file holds shared project resources.

# UNDERSTAND: uniform_bucket_level_access = true means access is controlled
# by IAM only, not per-object ACLs. Simpler and more secure.
resource "google_storage_bucket" "seed_data" {
  name          = var.seed_bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}