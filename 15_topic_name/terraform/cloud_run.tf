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

# Cloud Run service for CMEK data processing
resource "google_cloud_run_v2_service" "cmek_processor" {
  depends_on = [
    google_project_service.required_apis,
    google_service_account.cloud_run_sa
  ]
  
  name     = local.cloud_run_name
  location = local.region
  
  template {
    service_account = google_service_account.cloud_run_sa.email
    
    scaling {
      min_instance_count = 0
      max_instance_count = 5
    }
    
    containers {
      # Placeholder image - we'll build and deploy our custom image later
      image = "us-docker.pkg.dev/cloudrun/container/hello"
      
      ports {
        container_port = 8080
      }
      
      # Environment variables for the service
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
      
      # Resource limits
      resources {
        limits = {
          cpu    = "2"
          memory = "4Gi"
        }
        cpu_idle = true
      }
    }
    
    # Timeout for long-running encryption operations
    timeout = "3600s"
  }
  
  traffic {
    percent = 100
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  }
  
  labels = merge(local.common_labels, {
    component = "data-processor"
    encryption = "cmek"
  })
}

# Allow unauthenticated invocations for testing (remove in production)
resource "google_cloud_run_v2_service_iam_binding" "noauth" {
  location = google_cloud_run_v2_service.cmek_processor.location
  name     = google_cloud_run_v2_service.cmek_processor.name
  role     = "roles/run.invoker"
  members = [
    "allUsers"
  ]
}

# Cloud Scheduler job to trigger data processing
resource "google_cloud_scheduler_job" "cmek_processor_trigger" {
  depends_on = [google_project_service.required_apis]
  
  name        = "${local.prefix}-scheduler"
  description = "Trigger CMEK data processing pipeline"
  schedule    = "0 9 * * *" # Daily at 9 AM
  time_zone   = "UTC"
  region      = local.region

  http_target {
    http_method = "POST"
    uri         = "${google_cloud_run_v2_service.cmek_processor.uri}/process"
    
    headers = {
      "Content-Type" = "application/json"
    }
    
    body = base64encode(jsonencode({
      action = "daily_processing"
      validate_encryption = true
      run_performance_test = true
    }))
    
    oidc_token {
      service_account_email = google_service_account.cloud_run_sa.email
      audience             = google_cloud_run_v2_service.cmek_processor.uri
    }
  }
}

# Outputs moved to outputs.tf to avoid duplicates 