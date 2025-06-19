# Variables for CMEK POC Infrastructure

variable "project_id" {
  description = "The GCP project ID where resources will be created"
  type        = string
  
  validation {
    condition     = length(var.project_id) > 0
    error_message = "Project ID must not be empty."
  }
}

variable "region" {
  description = "The GCP region for regional resources"
  type        = string
  default     = "us-central1"
  
  validation {
    condition = contains([
      "us-central1", "us-east1", "us-west1", "us-west2",
      "europe-west1", "europe-west2", "europe-west3",
      "asia-east1", "asia-southeast1", "asia-northeast1"
    ], var.region)
    error_message = "Region must be a valid GCP region."
  }
}

variable "dataset_owner_email" {
  description = "Email address of the BigQuery dataset owner"
  type        = string
  
  validation {
    condition     = can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.dataset_owner_email))
    error_message = "Dataset owner email must be a valid email address."
  }
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "demo"
  
  validation {
    condition     = contains(["dev", "staging", "prod", "demo"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod, demo."
  }
}

variable "key_rotation_period" {
  description = "KMS key rotation period in seconds (default: 90 days)"
  type        = string
  default     = "7776000" # 90 days
  
  validation {
    condition     = can(tonumber(var.key_rotation_period)) && tonumber(var.key_rotation_period) >= 86400
    error_message = "Key rotation period must be at least 86400 seconds (1 day)."
  }
}

variable "enable_monitoring" {
  description = "Enable monitoring and alerting for the infrastructure"
  type        = bool
  default     = true
}

variable "storage_lifecycle_age" {
  description = "Number of days after which to delete objects in storage bucket"
  type        = number
  default     = 90
  
  validation {
    condition     = var.storage_lifecycle_age > 0
    error_message = "Storage lifecycle age must be a positive number."
  }
}

variable "cloud_run_min_instances" {
  description = "Minimum number of Cloud Run instances"
  type        = number
  default     = 0
  
  validation {
    condition     = var.cloud_run_min_instances >= 0
    error_message = "Minimum instances must be 0 or greater."
  }
}

variable "cloud_run_max_instances" {
  description = "Maximum number of Cloud Run instances"
  type        = number
  default     = 5
  
  validation {
    condition     = var.cloud_run_max_instances >= 1
    error_message = "Maximum instances must be at least 1."
  }
}

variable "cloud_run_cpu" {
  description = "CPU allocation for Cloud Run instances"
  type        = string
  default     = "2"
  
  validation {
    condition     = contains(["1", "2", "4", "8"], var.cloud_run_cpu)
    error_message = "CPU must be one of: 1, 2, 4, 8."
  }
}

variable "cloud_run_memory" {
  description = "Memory allocation for Cloud Run instances"
  type        = string
  default     = "4Gi"
  
  validation {
    condition = contains([
      "512Mi", "1Gi", "2Gi", "4Gi", "8Gi", "16Gi", "32Gi"
    ], var.cloud_run_memory)
    error_message = "Memory must be one of: 512Mi, 1Gi, 2Gi, 4Gi, 8Gi, 16Gi, 32Gi."
  }
}

variable "scheduler_frequency" {
  description = "Cron schedule for the Cloud Scheduler job"
  type        = string
  default     = "0 9 * * *" # Daily at 9 AM UTC
}

# Output all variables for reference
output "configuration_summary" {
  description = "Summary of the configuration used"
  value = {
    project_id              = var.project_id
    region                  = var.region
    environment            = var.environment
    key_rotation_period    = var.key_rotation_period
    enable_monitoring      = var.enable_monitoring
    storage_lifecycle_age  = var.storage_lifecycle_age
    cloud_run_min_instances = var.cloud_run_min_instances
    cloud_run_max_instances = var.cloud_run_max_instances
    cloud_run_cpu          = var.cloud_run_cpu
    cloud_run_memory       = var.cloud_run_memory
    scheduler_frequency    = var.scheduler_frequency
  }
} 