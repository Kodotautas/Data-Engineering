# POC: Customer-Managed Encryption Keys (CMEK) for BigQuery Data Pipeline

## 🎯 POC Objective
Build a secure data pipeline demonstrating end-to-end encryption using Customer-Managed Encryption Keys (CMEK) in BigQuery, showcasing how to protect sensitive data while maintaining performance.

## 🔧 What We'll Build
A complete data pipeline that:
1. **Creates a Cloud KMS key ring and key** for encryption management
2. **Sets up a BigQuery dataset with CMEK encryption** 
3. **Implements automated data ingestion** with encryption validation
4. **Demonstrates key rotation** and security monitoring
5. **Shows performance comparison** between default and CMEK encryption

## 🏗️ Architecture Components
```
Data Source → Cloud Storage (CMEK) → BigQuery (CMEK) → Analytics
                    ↓
           Cloud KMS Key Management
                    ↓
              Monitoring & Auditing
```

## 🛠️ Tech Stack
- **Google Cloud Platform**: BigQuery, Cloud KMS, Cloud Storage
- **Core Programming Language**: Rust (utilizing GCP Rust SDKs like `google-cloud-bigquery`, `google-cloud-kms`, `google-cloud-storage`)
- **Infrastructure**: Terraform for reproducible setup
- **Containerization & Orchestration**: Docker, Cloud Run, Cloud Scheduler
- **Monitoring**: Cloud Logging, Cloud Monitoring
- **CI/CD**: Cloud Build (for building Rust applications/containers)

## 🔧 Pipeline Building Tools & Architecture

### Core Pipeline Components
1. **Terraform** - Infrastructure as Code
   - KMS key rings and keys setup
   - BigQuery datasets with CMEK configuration
   - IAM roles and permissions
   - Cloud Storage buckets with encryption

2. **Rust Applications with GCP SDKs** - Data Processing & Encryption Logic
   - Utilize Rust crates like `google-cloud-bigquery`, `google-cloud-kms`, `google-cloud-storage`.
   - Custom logic for data ingestion and validation (including CMEK checks).
   - Can be containerized using Docker and deployed on Cloud Run for serverless execution.
   - Offers performance benefits and memory safety for data handling.

3. **Cloud Run + Cloud Scheduler** - Orchestration & Automation
   - Deploy containerized Rust applications as Cloud Run services.
   - Use Cloud Scheduler to trigger services for pipeline execution (e.g., data ingestion).
   - Provides a serverless, scalable way to manage workflows.

### Sample Pipeline Flow
```
1. CSV/JSON files → Cloud Storage (CMEK) with sensitive data
2. Cloud Scheduler triggers Cloud Run service (hosting a Rust application)
3. Cloud Run Rust job (application) processes data from Cloud Storage, validates encryption using Cloud KMS, and interacts with BigQuery client libraries.
4. Data is loaded into the BigQuery dataset (CMEK) by the Rust application which is encripted.