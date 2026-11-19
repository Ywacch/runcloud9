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
  description = "GKE cluster name (Autopilot regional and Standard zonal may share this name)"
  type        = string
  default     = "cloud9"
}

variable "zone" {
  description = "GCP zone for the zonal Standard cluster (GS-1)"
  type        = string
  default     = "us-central1-a"
}

variable "session_machine_type" {
  description = "Machine type for the Standard session node pool"
  type        = string
  default     = "e2-standard-8"
}

variable "session_pool_max_nodes" {
  description = "Maximum nodes in the Standard session pool (about five probe sessions per e2-standard-8)"
  type        = number
  default     = 400
}

variable "standard_subnet_cidr" {
  description = "Primary CIDR for Standard cluster nodes (/21 for up to 2,048 node IPs)"
  type        = string
  default     = "10.0.0.0/21"
}

variable "standard_pods_cidr" {
  description = "Secondary range name pods: CIDR for pod IPs (/15 at 64 pods per node)"
  type        = string
  default     = "10.4.0.0/15"
}

variable "standard_services_cidr" {
  description = "Secondary range name services: CIDR for ClusterIP (/18 for 2,000 sessions)"
  type        = string
  default     = "10.12.0.0/18"
}

variable "standard_master_cidr" {
  description = "RFC1918 control plane peering range for the private Standard cluster"
  type        = string
  default     = "172.16.0.0/28"
}

variable "seed_bucket_name" {
  description = "GCS bucket for seed data SQL dumps"
  type        = string
  default     = "cloud9-seed-data"
}
