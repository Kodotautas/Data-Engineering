# BigQuery Resources with CMEK
# Secure data warehouse with customer-managed encryption

# Wait for IAM propagation before creating BigQuery resources
resource "time_sleep" "wait_for_bigquery_iam" {
  depends_on = [google_kms_crypto_key_iam_binding.cmek_services]
  create_duration = "60s"
}

# BigQuery Dataset with CMEK encryption
resource "google_bigquery_dataset" "cmek_dataset" {
  dataset_id  = "cmek_poc_dataset"
  location    = var.region
  description = "Dataset demonstrating Customer-Managed Encryption Keys (CMEK)"

  # Enable CMEK encryption
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.cmek_key.id
  }

  # Set appropriate access controls
  access {
    role          = "OWNER"
    user_by_email = var.dataset_owner_email
  }

  access {
    role         = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role         = "READER"
    special_group = "projectReaders"
  }

  labels = {
    environment = var.environment
    encryption  = "cmek"
    purpose     = "demo"
  }

  depends_on = [time_sleep.wait_for_bigquery_iam]
}

# Customers table schema
resource "google_bigquery_table" "customers" {
  dataset_id = google_bigquery_dataset.cmek_dataset.dataset_id
  table_id   = "customers"

  description = "Customer information table with PII data"

  # Table-level encryption (inherits from dataset)
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.cmek_key.id
  }

  schema = jsonencode([
    {
      name        = "customer_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique customer identifier"
    },
    {
      name        = "email"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer email address (PII)"
    },
    {
      name        = "full_name"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer full name (PII)"
    },
    {
      name        = "phone_number"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Customer phone number (PII)"
    },
    {
      name        = "registration_date"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Account registration timestamp"
    },
    {
      name        = "country"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer country"
    }
  ])

  labels = {
    environment = var.environment
    encryption  = "cmek"
    data_type   = "pii"
  }

  deletion_protection = false
}

# Transactions table schema
resource "google_bigquery_table" "transactions" {
  dataset_id = google_bigquery_dataset.cmek_dataset.dataset_id
  table_id   = "transactions"

  description = "Financial transaction records"

  # Table-level encryption (inherits from dataset)
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.cmek_key.id
  }

  schema = jsonencode([
    {
      name        = "transaction_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique transaction identifier"
    },
    {
      name        = "customer_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Customer identifier (foreign key)"
    },
    {
      name        = "amount"
      type        = "FLOAT64"
      mode        = "REQUIRED"
      description = "Transaction amount"
    },
    {
      name        = "currency"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Transaction currency"
    },
    {
      name        = "transaction_date"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Transaction timestamp"
    },
    {
      name        = "merchant_name"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Merchant name"
    },
    {
      name        = "category"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Transaction category"
    }
  ])

  labels = {
    environment = var.environment
    encryption  = "cmek"
    data_type   = "financial"
  }

  deletion_protection = false
}

# Performance metrics table for tracking CMEK performance
resource "google_bigquery_table" "performance_metrics" {
  dataset_id = google_bigquery_dataset.cmek_dataset.dataset_id
  table_id   = "performance_metrics"

  description = "Performance metrics for CMEK operations"

  # Table-level encryption (inherits from dataset)
  encryption_configuration {
    kms_key_name = google_kms_crypto_key.cmek_key.id
  }

  schema = jsonencode([
    {
      name        = "metric_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique metric identifier"
    },
    {
      name        = "test_run_date"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "When the test was executed"
    },
    {
      name        = "encryption_type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Type of encryption used (CMEK, DEFAULT)"
    },
    {
      name        = "operation_type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Type of operation measured"
    },
    {
      name        = "execution_time_ms"
      type        = "INTEGER"
      mode        = "REQUIRED"
      description = "Execution time in milliseconds"
    },
    {
      name        = "rows_processed"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Number of rows processed"
    },
    {
      name        = "data_size_bytes"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Data size in bytes"
    }
  ])

  labels = {
    environment = var.environment
    encryption  = "cmek"
    purpose     = "metrics"
  }

  deletion_protection = false
}

# Outputs moved to outputs.tf to avoid duplicates 