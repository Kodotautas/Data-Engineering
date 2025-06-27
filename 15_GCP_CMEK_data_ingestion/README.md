# GCP CMEK Data Ingestion

Secure data ingestion pipeline using Customer-Managed Encryption Keys (CMEK) for data at rest encryption.

## What It Does

This project demonstrates secure data processing with CMEK encryption:
- Encrypts data at rest using customer-managed encryption keys
- Generates and processes sample customer and transaction data
- Stores encrypted data in BigQuery and Cloud Storage
- Provides automated data pipeline with Cloud Run and Rust

## Infrastructure

The deployment creates:
- **Cloud KMS**: Key ring and encryption key for CMEK
- **BigQuery**: Dataset with CMEK encryption
- **Cloud Storage**: Bucket with CMEK encryption  
- **Cloud Run**: Rust application for data processing
- **Cloud Scheduler**: Automated pipeline execution
- **IAM**: Service accounts and permissions

## Prerequisites

- Google Cloud CLI (`gcloud`)
- Terraform
- Docker (optional - Cloud Build handles containers)

## Quick Deploy

1. **Authenticate with GCP:**
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Run one-step deployment:**
   ```bash
   ./deploy_cmek_one_step.sh
   ```

3. **Follow the prompts:**
   - Enter your GCP Project ID
   - Enter your email address
   - Select deployment region

## Manual Deploy

If you prefer manual deployment:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
terraform init
terraform plan
terraform apply
```

## Testing

Run the demo to test the pipeline:
```bash
./demo.sh
```

## Sample Data

The application generates:
- 1,000 customer records (PII: emails, names, phone numbers)
- 5,000 transaction records (financial data)
- All data encrypted with CMEK

## Cleanup

To destroy all resources:
```bash
cd terraform
terraform destroy
```
