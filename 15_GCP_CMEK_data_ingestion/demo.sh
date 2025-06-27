#!/bin/bash

# CMEK POC Demo Script
# This script demonstrates the one-step deployment process

echo "🚀 CMEK POC - One-Step Deployment Demo"
echo "======================================"
echo

echo "📋 Prerequisites Check:"
echo "1. Google Cloud CLI (gcloud) - $(command -v gcloud >/dev/null && echo '✅ Installed' || echo '❌ Not found')"
echo "2. Terraform - $(command -v terraform >/dev/null && echo '✅ Installed' || echo '❌ Not found')"
echo "3. Docker - $(command -v docker >/dev/null && echo '✅ Installed' || echo '❌ Not found')"
echo

echo "🔐 What CMEK Does:"
echo "- Encrypts your data at rest with customer-managed keys"
echo "- Protects against unauthorized access to stored data"
echo "- Maintains full data readability when properly decrypted"
echo "- Provides automatic key rotation and security monitoring"
echo

echo "🏗️ What Gets Deployed:"
echo "1. Cloud KMS key ring and encryption key"
echo "2. BigQuery dataset with CMEK encryption"
echo "3. Cloud Storage bucket with CMEK encryption"
echo "4. Cloud Run application for data processing"
echo "5. Cloud Scheduler for automation"
echo "6. IAM service accounts and permissions"
echo

echo "📊 Sample Data Generated:"
echo "- 1,000 customer records (with PII: emails, names, phone numbers)"
echo "- 5,000 transaction records (financial data)"
echo "- All encrypted with CMEK and stored securely"
echo

echo "🧪 Testing Included:"
echo "- Health check validation"
echo "- CMEK encryption verification"
echo "- Data processing performance metrics"
echo "- End-to-end pipeline testing"
echo

echo "💡 To Deploy Everything:"
echo "1. Make sure you're authenticated with Google Cloud:"
echo "   gcloud auth login"
echo "   gcloud config set project YOUR_PROJECT_ID"
echo
echo "2. Run the one-step deployment:"
echo "   ./deploy_cmek_one_step.sh"
echo

echo "🎯 Expected Timeline:"
echo "- Infrastructure setup: ~5-10 minutes"
echo "- Application deployment: ~3-5 minutes"
echo "- Testing and validation: ~2-3 minutes"
echo "- Total time: ~10-15 minutes"
echo

echo "💰 Cost Estimate:"
echo "- Cloud KMS: ~$1-2/month"
echo "- BigQuery: ~$5-10/month (depending on usage)"
echo "- Cloud Storage: ~$2-5/month"
echo "- Cloud Run: ~$5-15/month"
echo "- Total: ~$15-30/month"
echo "- Clean up with: cd terraform && terraform destroy"
echo

echo "🔧 Ready to Deploy?"
echo "Run: ./deploy_cmek_one_step.sh"
echo
echo "📚 For more details, see: README_ONE_STEP.md" 