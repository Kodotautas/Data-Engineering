# Cloud Run Resources
# Containerized service for CMEK data processing and validation

# Service account for Cloud Run
resource "google_service_account" "cloud_run_sa" {
  account_id   = local.cloud_run_sa_name
  display_name = "CMEK POC Cloud Run Service Account"
  description  = "Service account for Cloud Run CMEK data processing"
}

# IAM roles for the Cloud Run service account
resource "google_project_iam_member" "cloud_run_bigquery" {
  project = local.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_bigquery_job" {
  project = local.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_storage" {
  project = local.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_storage_admin" {
  project = local.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_logging" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

resource "google_project_iam_member" "cloud_run_monitoring" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# Cloud Run Jobs service account permissions for job creation
resource "google_project_iam_member" "cloud_run_jobs_run" {
  project = local.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.cloud_run_sa.email}"
}

# Cloud Run job for CMEK data processing
resource "google_cloud_run_v2_job" "cmek_processor" {
  depends_on = [
    google_project_service.required_apis,
    google_service_account.cloud_run_sa
  ]
  
  name     = local.cloud_run_name
  location = local.region
  
  template {
    task_count = 1
    parallelism = 1
    
    template {
      service_account = google_service_account.cloud_run_sa.email
      
      containers {
        # Placeholder image - we'll build and deploy our custom image later
        image = "us-docker.pkg.dev/cloudrun/container/hello"
        
        # Environment variables for the job
        env {
          name  = "PROJECT_ID"
          value = local.project_id
        }
        
        env {
          name  = "REGION"
          value = local.region
        }
        
        env {
          name  = "KMS_KEY_ID"
          value = google_kms_crypto_key.cmek_key.id
        }
        
        env {
          name  = "BIGQUERY_DATASET"
          value = google_bigquery_dataset.cmek_dataset.dataset_id
        }
        
        env {
          name  = "STORAGE_BUCKET"
          value = google_storage_bucket.data_bucket.name
        }
        
        env {
          name  = "ACTION"
          value = "daily_processing"
        }
        
        env {
          name  = "VALIDATE_ENCRYPTION"
          value = "true"
        }
        
        env {
          name  = "RUN_PERFORMANCE_TEST"
          value = "true"
        }
        
        env {
          name  = "CUSTOMERS_COUNT"
          value = "1000"
        }
        
        env {
          name  = "TRANSACTIONS_COUNT"
          value = "5000"
        }
        
        # Resource limits
        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
      }
    }
  }
  
  labels = merge(local.common_labels, {
    component = "data-processor"
    encryption = "cmek"
  })
}

# IAM binding to allow the service account to run the job
resource "google_cloud_run_v2_job_iam_binding" "job_runner" {
  name     = google_cloud_run_v2_job.cmek_processor.name
  location = google_cloud_run_v2_job.cmek_processor.location
  role     = "roles/run.invoker"
  members = [
    "serviceAccount:${google_service_account.cloud_run_sa.email}"
  ]
}

# Cloud Scheduler job to trigger data processing
resource "google_cloud_scheduler_job" "cmek_processor_trigger" {
  depends_on = [
    google_project_service.required_apis,
    google_cloud_run_v2_job.cmek_processor
  ]
  
  name        = "${local.prefix}-scheduler"
  description = "Trigger CMEK data processing pipeline"
  schedule    = "0 9 * * *" # Daily at 9 AM
  time_zone   = "UTC"
  region      = local.region

  http_target {
    http_method = "POST"
    uri         = "https://${local.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${local.project_id}/jobs/${google_cloud_run_v2_job.cmek_processor.name}:run"
    
    headers = {
      "Content-Type" = "application/json"
    }
    
    oidc_token {
      service_account_email = google_service_account.cloud_run_sa.email
      audience             = "https://${local.region}-run.googleapis.com/"
    }
  }
}

# Outputs moved to outputs.tf to avoid duplicates 