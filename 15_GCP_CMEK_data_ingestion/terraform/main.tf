# CMEK POC Infrastructure
# Customer-Managed Encryption Keys with BigQuery and Cloud Run

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Local values for consistent naming
locals {
  project_id = var.project_id
  region     = var.region
  
  # Naming conventions
  prefix = "cmek-poc"
  
  # Resource names
  kms_keyring_name    = "${local.prefix}-keyring"
  kms_key_name        = "${local.prefix}-key"
  bigquery_dataset_id = "${replace(local.prefix, "-", "_")}_dataset"
  storage_bucket_name = "${local.prefix}-data-${random_id.bucket_suffix.hex}"
  cloud_run_name      = "${local.prefix}-processor"
  
  # Service accounts
  cloud_run_sa_name = "${local.prefix}-run-sa"
  
  common_labels = {
    project     = "cmek-poc"
    environment = "demo"
    created_by  = "terraform"
  }
}

# Random suffix for globally unique bucket name
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "cloudkms.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com"
  ])
  
  service = each.key
  
  disable_dependent_services = false
  disable_on_destroy        = false
}

# Wait for APIs to be enabled
resource "time_sleep" "wait_for_apis" {
  depends_on = [google_project_service.required_apis]
  create_duration = "30s"
} 