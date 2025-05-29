# Terraform Outputs - CMEK POC Infrastructure
# All important resource information for easy access

# KMS Outputs
output "kms_key_ring_name" {
  description = "The name of the KMS key ring"
  value       = google_kms_key_ring.cmek_keyring.name
}

output "kms_key_id" {
  description = "The full ID of the KMS encryption key"
  value       = google_kms_crypto_key.cmek_key.id
}

output "kms_key_name" {
  description = "The name of the KMS encryption key"
  value       = google_kms_crypto_key.cmek_key.name
}

# Storage Outputs
output "storage_bucket_name" {
  description = "The name of the encrypted Cloud Storage bucket"
  value       = google_storage_bucket.data_bucket.name
}

output "storage_bucket_url" {
  description = "The URL of the encrypted Cloud Storage bucket"
  value       = google_storage_bucket.data_bucket.url
}

# BigQuery Outputs
output "bigquery_dataset_id" {
  description = "The ID of the encrypted BigQuery dataset"
  value       = google_bigquery_dataset.cmek_dataset.dataset_id
}

output "bigquery_dataset_location" {
  description = "The location of the BigQuery dataset"
  value       = google_bigquery_dataset.cmek_dataset.location
}

output "bigquery_tables" {
  description = "List of created BigQuery tables"
  value = {
    customers          = google_bigquery_table.customers.table_id
    transactions       = google_bigquery_table.transactions.table_id
    performance_metrics = google_bigquery_table.performance_metrics.table_id
  }
}

# Cloud Run Outputs
output "cloud_run_url" {
  description = "The URL of the Cloud Run service"
  value       = google_cloud_run_v2_service.cmek_processor.uri
}

output "cloud_run_service_name" {
  description = "The name of the Cloud Run service"
  value       = google_cloud_run_v2_service.cmek_processor.name
}

output "cloud_run_service_account_email" {
  description = "The email of the Cloud Run service account"
  value       = google_service_account.cloud_run_sa.email
}

# Cloud Scheduler Output
output "scheduler_job_name" {
  description = "The name of the Cloud Scheduler job"
  value       = google_cloud_scheduler_job.cmek_processor_trigger.name
}

# Configuration Summary
output "deployment_summary" {
  description = "Summary of the deployed CMEK POC infrastructure"
  value = {
    project_id     = var.project_id
    region         = var.region
    environment    = var.environment
    
    # KMS Information
    kms_key_ring   = google_kms_key_ring.cmek_keyring.name
    kms_key        = google_kms_crypto_key.cmek_key.name
    
    # Data Storage
    storage_bucket = google_storage_bucket.data_bucket.name
    bigquery_dataset = google_bigquery_dataset.cmek_dataset.dataset_id
    
    # Processing
    cloud_run_service = google_cloud_run_v2_service.cmek_processor.name
    cloud_run_url     = google_cloud_run_v2_service.cmek_processor.uri
    
    # Automation
    scheduler_job = google_cloud_scheduler_job.cmek_processor_trigger.name
  }
}

# Quick Access Commands
output "useful_commands" {
  description = "Useful commands for interacting with the deployed resources"
  value = {
    # BigQuery commands
    bigquery_query_example = "bq query --use_legacy_sql=false 'SELECT * FROM `${var.project_id}.${google_bigquery_dataset.cmek_dataset.dataset_id}.customers` LIMIT 10'"
    
    # Cloud Storage commands
    storage_list_files = "gsutil ls gs://${google_storage_bucket.data_bucket.name}/"
    storage_upload_sample = "gsutil cp sample-data.csv gs://${google_storage_bucket.data_bucket.name}/sample-data/"
    
    # Cloud Run commands
    cloud_run_logs = "gcloud run services logs read ${google_cloud_run_v2_service.cmek_processor.name} --region=${var.region}"
    cloud_run_describe = "gcloud run services describe ${google_cloud_run_v2_service.cmek_processor.name} --region=${var.region}"
    
    # KMS commands
    kms_key_info = "gcloud kms keys describe ${google_kms_crypto_key.cmek_key.name} --keyring=${google_kms_key_ring.cmek_keyring.name} --location=${var.region}"
    kms_list_versions = "gcloud kms keys versions list --key=${google_kms_crypto_key.cmek_key.name} --keyring=${google_kms_key_ring.cmek_keyring.name} --location=${var.region}"
    
    # Scheduler commands
    scheduler_run_now = "gcloud scheduler jobs run ${google_cloud_scheduler_job.cmek_processor_trigger.name} --location=${var.region}"
    scheduler_logs = "gcloud scheduler jobs describe ${google_cloud_scheduler_job.cmek_processor_trigger.name} --location=${var.region}"
  }
}

# Security Information
output "security_info" {
  description = "Important security information about the deployment"
  value = {
    kms_key_rotation_enabled = "Automatic rotation every 90 days"
    encryption_at_rest = "All data encrypted with customer-managed keys"
    service_account = google_service_account.cloud_run_sa.email
    
    # Verification commands
    verify_bigquery_encryption = "Check BigQuery dataset encryption in console or use: bq show --encryption_service_account --format=prettyjson ${var.project_id}:${google_bigquery_dataset.cmek_dataset.dataset_id}"
    verify_storage_encryption = "Check bucket encryption: gsutil kms encryption gs://${google_storage_bucket.data_bucket.name}"
  }
} 