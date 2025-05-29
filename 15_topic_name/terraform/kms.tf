# Cloud KMS Resources for CMEK
# Creates key ring and encryption keys for BigQuery and Cloud Storage

# KMS Key Ring
resource "google_kms_key_ring" "cmek_keyring" {
  name     = "cmek-poc-keyring"
  location = var.region

  depends_on = [time_sleep.wait_for_apis]
}

# KMS Encryption Key with automatic rotation
resource "google_kms_crypto_key" "cmek_key" {
  name     = "cmek-poc-key"
  key_ring = google_kms_key_ring.cmek_keyring.id
  purpose  = "ENCRYPT_DECRYPT"

  rotation_period = "7776000s"  # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

# IAM binding for key administrators
resource "google_kms_crypto_key_iam_binding" "key_admin" {
  crypto_key_id = google_kms_crypto_key.cmek_key.id
  role          = "roles/cloudkms.admin"

  members = [
    "user:${var.dataset_owner_email}"
  ]
}

# Combined IAM binding for all services that need to encrypt/decrypt
resource "google_kms_crypto_key_iam_binding" "cmek_services" {
  crypto_key_id = google_kms_crypto_key.cmek_key.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"

  members = [
    "serviceAccount:bq-${data.google_project.current.number}@bigquery-encryption.iam.gserviceaccount.com",
    "serviceAccount:service-${data.google_project.current.number}@gs-project-accounts.iam.gserviceaccount.com",
    "serviceAccount:${google_service_account.cloud_run_sa.email}",
  ]
}

# Data source to get current project information
data "google_project" "current" {}

# Outputs moved to outputs.tf to avoid duplicates 