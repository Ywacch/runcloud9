variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "ganymede-cloud"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "cluster_name" {
  description = "GKE Autopilot cluster name"
  type        = string
  default     = "cloud9"
}

variable "seed_bucket_name" {
  description = "GCS bucket for seed data SQL dumps"
  type        = string
  default     = "cloud9-seed-data"
}
