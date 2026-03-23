output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.ganymede.name
}

output "cluster_location" {
  description = "GKE cluster location"
  value       = google_container_cluster.ganymede.location
}

output "seed_bucket_name" {
  description = "GCS bucket name for seed data"
  value       = google_storage_bucket.seed_data.name
}

output "kubeconfig_command" {
  description = "Command to configure kubectl for this cluster"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.ganymede.name} --region ${var.region} --project ${var.project_id}"
}
