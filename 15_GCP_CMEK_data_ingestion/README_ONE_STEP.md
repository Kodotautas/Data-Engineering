# 🚀 CMEK POC - One-Step Deployment

Deploy the complete Customer-Managed Encryption Keys (CMEK) proof-of-concept with a single command!

## 🎯 What This Does

This one-step deployment script automatically:

1. **🔧 Validates Requirements** - Checks for required tools (Terraform, gcloud, Docker)
2. **🔐 Sets Up Infrastructure** - Creates KMS keys, BigQuery dataset, Cloud Storage bucket with CMEK
3. **🚀 Deploys Application** - Builds and deploys the Cloud Run application
4. **🧪 Tests Everything** - Validates CMEK setup and data processing
5. **📊 Provides Results** - Shows you all the endpoints and resources created

## ⚡ Quick Start

### Prerequisites

1. **Google Cloud CLI** installed and authenticated:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Terraform** installed:
   ```bash
   # Ubuntu/Debian
   curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
   sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs)"
   sudo apt-get update && sudo apt-get install terraform
   
   # macOS
   brew install terraform
   ```

3. **Docker** installed and running

### 🚀 Deploy Everything

```bash
# Make the script executable
chmod +x deploy_cmek_one_step.sh

# Run the one-step deployment
./deploy_cmek_one_step.sh
```

The script will:
- Ask for your GCP project ID and email
- Let you choose a region
- Show you what will be created
- Deploy everything automatically
- Test the deployment
- Give you all the URLs and commands to use

## 📋 What Gets Created

- **🔐 Cloud KMS**: Key ring and encryption key with automatic rotation
- **📊 BigQuery**: Dataset and tables with CMEK encryption
- **💾 Cloud Storage**: Encrypted bucket for data storage
- **🚀 Cloud Run**: Containerized data processing service
- **⏰ Cloud Scheduler**: Automated pipeline triggering
- **🔑 IAM**: Service accounts with least-privilege access

## 🧪 Testing Your Deployment

After deployment, you can test with these commands:

```bash
# Get your service URL from the deployment output
SERVICE_URL="https://your-service-url"

# Health check
curl $SERVICE_URL/

# Validate CMEK setup
curl $SERVICE_URL/validate

# Process test data
curl -X POST $SERVICE_URL/process \
  -H "Content-Type: application/json" \
  -d '{"action": "test_data_generation"}'

# Get performance metrics
curl $SERVICE_URL/metrics
```

## 📊 Expected Results

You should see:
- ✅ **Health check**: Service is running
- ✅ **CMEK validation**: All resources encrypted with customer-managed keys
- ✅ **Data processing**: 1,000 customers and 5,000 transactions created
- ✅ **Performance metrics**: Upload and query times recorded

## 🧹 Cleanup

To remove all resources and avoid charges:

```bash
cd terraform
terraform destroy
```

## 🔧 Troubleshooting

### Common Issues

1. **"Not authenticated with Google Cloud"**
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **"Missing required tools"**
   - Install Terraform, gcloud, and Docker
   - Make sure Docker is running

3. **"Permission denied"**
   - Ensure your account has the required IAM roles:
     - `roles/owner` OR the following granular roles:
     - `roles/cloudkms.admin`
     - `roles/bigquery.admin`
     - `roles/storage.admin`
     - `roles/run.admin`
     - `roles/cloudscheduler.admin`
     - `roles/iam.serviceAccountAdmin`

4. **"API not enabled"**
   - The script will automatically enable required APIs
   - If it fails, manually enable:
     - Cloud KMS API
     - BigQuery API
     - Cloud Storage API
     - Cloud Run API
     - Cloud Build API
     - Cloud Scheduler API

### Getting Help

If the deployment fails:
1. Check the error message
2. Try running the script again
3. If it still fails, clean up with `cd terraform && terraform destroy`
4. Check the detailed logs in the terraform and app directories

## 🎉 Success!

Once deployed, you'll have a complete CMEK implementation that demonstrates:
- **End-to-end encryption** with customer-managed keys
- **Performance monitoring** of encryption operations
- **Automated data processing** with security validation
- **Production-ready architecture** for secure data pipelines

Perfect for learning CMEK or as a reference implementation for your own projects! 🚀 