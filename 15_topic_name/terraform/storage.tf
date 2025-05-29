# Cloud Storage Resources with CMEK
# Secure data storage with customer-managed encryption

# Cloud Storage bucket with CMEK encryption
resource "google_storage_bucket" "data_bucket" {
  name          = "cmek-poc-data-${random_id.bucket_suffix.hex}"
  location      = var.region
  force_destroy = true

  depends_on = [google_kms_crypto_key_iam_binding.cmek_services]

  # Enable CMEK encryption
  encryption {
    default_kms_key_name = google_kms_crypto_key.cmek_key.id
  }

  # Versioning for data protection
  versioning {
    enabled = true
  }

  # Lifecycle management
  lifecycle_rule {
    condition {
      age = var.storage_lifecycle_age
    }
    action {
      type = "Delete"
    }
  }

  # Uniform bucket-level access
  uniform_bucket_level_access = true

  labels = {
    environment = var.environment
    encryption  = "cmek"
    purpose     = "data-storage"
  }
}

# IAM binding for Cloud Run service account to access bucket
resource "google_storage_bucket_iam_binding" "bucket_access" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.admin"

  members = [
    "serviceAccount:${google_service_account.cloud_run_sa.email}",
  ]
}

# Bucket objects will be added after IAM is properly configured
# Sample data folder
# resource "google_storage_bucket_object" "sample_data_folder" {
#   name   = "sample-data/"
#   bucket = google_storage_bucket.data_bucket.name
#   source = "/dev/null"
# }

# Processed data folder
# resource "google_storage_bucket_object" "processed_data_folder" {
#   name   = "processed-data/"
#   bucket = google_storage_bucket.data_bucket.name
#   source = "/dev/null"
# }

# Logs folder
# resource "google_storage_bucket_object" "logs_folder" {
#   name   = "logs/"
#   bucket = google_storage_bucket.data_bucket.name
#   source = "/dev/null"
# }

# Outputs moved to outputs.tf to avoid duplicates 