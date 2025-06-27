# CMEK POC Infrastructure - Terraform

This Terraform configuration deploys a complete **Customer-Managed Encryption Keys (CMEK)** proof-of-concept infrastructure on Google Cloud Platform, showcasing secure data pipelines with Cloud Run, BigQuery, and Cloud Storage.

## 🏗️ Infrastructure Overview

### What Gets Deployed

- **🔐 Cloud KMS**: Key ring and encryption key with automatic rotation
- **📊 BigQuery**: Dataset and tables with CMEK encryption
- **💾 Cloud Storage**: Encrypted bucket for data storage
- **🚀 Cloud Run**: Containerized data processing service
- **⏰ Cloud Scheduler**: Automated pipeline triggering
- **🔑 IAM**: Service accounts with least-privilege access

### Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Cloud KMS     │◄───┤  Cloud Storage   │◄───┤   Cloud Run     │
│   (Key Ring)    │    │     (CMEK)       │    │   (Processor)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲                        │                       │
         │              ┌──────────────────┐             │
         └──────────────►│    BigQuery      │◄────────────┘
                         │     (CMEK)       │
                         └──────────────────┘
                                  ▲
                         ┌──────────────────┐
                         │ Cloud Scheduler  │
                         │   (Automation)   │
                         └──────────────────┘
```

## 🚀 Quick Start

### Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Terraform** >= 1.0 installed
3. **gcloud CLI** configured with appropriate permissions
4. **Required APIs** (will be enabled automatically):
   - Cloud KMS API
   - BigQuery API
   - Cloud Storage API
   - Cloud Run API
   - Cloud Build API
   - Cloud Scheduler API

### Required Permissions

Your account needs these IAM roles:
- `roles/owner` OR the following granular roles:
  - `roles/cloudkms.admin`
  - `roles/bigquery.admin`
  - `roles/storage.admin`
  - `roles/run.admin`
  - `roles/cloudscheduler.admin`
  - `roles/iam.serviceAccountAdmin`
  - `roles/serviceusage.serviceUsageAdmin`

### Deployment Steps

1. **Clone and navigate to the terraform directory**
   ```bash
   cd 15_topic_name/terraform
   ```

2. **Copy and configure variables**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```

3. **Edit `terraform.tfvars` with your values**
   ```hcl
   project_id = "your-gcp-project-id"
   dataset_owner_email = "your-email@domain.com"
   region = "us-central1"
   ```

4. **Initialize Terraform**
   ```bash
   terraform init
   ```

5. **Plan the deployment**
   ```bash
   terraform plan
   ```

6. **Apply the configuration**
   ```bash
   terraform apply
   ```

## 📋 Configuration Variables

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `project_id` | Your GCP project ID | `"my-gcp-project"` |
| `dataset_owner_email` | Email for BigQuery dataset ownership | `"user@company.com"` |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `region` | `"us-central1"` | GCP region for resources |
| `environment` | `"demo"` | Environment name |
| `key_rotation_period` | `"7776000"` | KMS key rotation (90 days) |
| `cloud_run_cpu` | `"2"` | Cloud Run CPU allocation |
| `cloud_run_memory` | `"4Gi"` | Cloud Run memory allocation |
| `scheduler_frequency` | `"0 9 * * *"` | Cron schedule for automation |

## 🔍 Verification Commands

After deployment, verify your infrastructure:

### Check KMS Key
```bash
gcloud kms keys describe cmek-poc-key \
  --keyring=cmek-poc-keyring \
  --location=us-central1
```

### Verify BigQuery Encryption
```bash
bq show --format=prettyjson your-project:cmek_poc_dataset
```

### Check Cloud Storage Encryption
```bash
gsutil kms encryption gs://cmek-poc-data-[suffix]
```

### Test Cloud Run Service
```bash
curl -X POST [CLOUD_RUN_URL]/process \
  -H "Content-Type: application/json" \
  -d '{"action": "test", "validate_encryption": true}'
```

## 📊 Outputs

After successful deployment, you'll get:

- **KMS Key Information**: Key ring and key names/IDs
- **Storage Details**: Bucket name and URL
- **BigQuery Resources**: Dataset ID and table names
- **Cloud Run Service**: URL and service account
- **Useful Commands**: Ready-to-use CLI commands
- **Security Info**: Encryption verification commands

## 🧹 Cleanup

To destroy all resources:

```bash
terraform destroy
```

⚠️ **Warning**: This will permanently delete all data and resources.

## 🔧 Customization

### Adjusting Encryption Settings

To change key rotation period:
```hcl
key_rotation_period = "2592000"  # 30 days
```

### Scaling Cloud Run

For higher performance:
```hcl
cloud_run_cpu = "4"
cloud_run_memory = "8Gi"
cloud_run_max_instances = 10
```

### Regional Deployment

For different regions:
```hcl
region = "europe-west1"
```

## 🔒 Security Best Practices

This infrastructure implements:

- **🔐 CMEK Encryption**: Customer-managed keys for all data
- **🔄 Automatic Key Rotation**: 90-day rotation policy
- **🎯 Least Privilege IAM**: Minimal required permissions
- **📝 Audit Logging**: Comprehensive access logging
- **🛡️ Network Security**: VPC-native where applicable

## 📝 Next Steps

After deployment:

1. **Deploy the Cloud Run Application** (see `/app` directory)
2. **Upload Sample Data** to test encryption
3. **Run Performance Tests** to gather metrics
4. **Configure Monitoring** dashboards
5. **Write Your LinkedIn Post** with real results!

## 🐛 Troubleshooting

### Common Issues

**API Not Enabled Error**
```bash
# Wait 30-60 seconds and retry
terraform apply
```

**Permissions Error**
```bash
# Check your IAM roles
gcloud projects get-iam-policy YOUR_PROJECT_ID
```

**KMS Key Access Error**
```bash
# Verify service account permissions
gcloud kms keys get-iam-policy cmek-poc-key \
  --keyring=cmek-poc-keyring \
  --location=us-central1
```

## 📚 Resources

- [Google Cloud KMS Documentation](https://cloud.google.com/kms/docs)
- [BigQuery Encryption](https://cloud.google.com/bigquery/docs/customer-managed-encryption)
- [Cloud Storage CMEK](https://cloud.google.com/storage/docs/encryption/customer-managed-keys)
- [Cloud Run Documentation](https://cloud.google.com/run/docs)

---

**💡 Tip**: Use the output commands to quickly interact with your deployed resources! 