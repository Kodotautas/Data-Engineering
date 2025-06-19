#!/bin/bash

# CMEK POC - Cloud Run Deployment Script (Updated for Artifact Registry)
# Builds and deploys the CMEK data processor application

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID=${PROJECT_ID:-"vl-data-learn"}
REGION=${REGION:-"europe-west1"}
SERVICE_NAME="cmek-poc-processor"
ARTIFACT_REGISTRY_REPO="cmek-poc"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}/${SERVICE_NAME}"

echo -e "${BLUE}🚀 CMEK POC - Cloud Run Deployment (Artifact Registry)${NC}"
echo -e "${BLUE}====================================================${NC}"
echo ""
echo -e "Project ID: ${GREEN}${PROJECT_ID}${NC}"
echo -e "Region: ${GREEN}${REGION}${NC}"
echo -e "Service: ${GREEN}${SERVICE_NAME}${NC}"
echo -e "Repository: ${GREEN}${ARTIFACT_REGISTRY_REPO}${NC}"
echo -e "Image: ${GREEN}${IMAGE_NAME}${NC}"
echo ""

# Check if gcloud is authenticated
echo -e "${YELLOW}🔐 Checking authentication...${NC}"
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo -e "${RED}❌ Not authenticated with gcloud. Please run 'gcloud auth login'${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Authentication verified${NC}"

# Set project
echo -e "${YELLOW}📝 Setting project...${NC}"
gcloud config set project ${PROJECT_ID}

# Enable required APIs
echo -e "${YELLOW}🔧 Enabling required APIs...${NC}"
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Create Artifact Registry repository if it doesn't exist
echo -e "${YELLOW}📦 Setting up Artifact Registry...${NC}"
if ! gcloud artifacts repositories describe ${ARTIFACT_REGISTRY_REPO} --location=${REGION} --quiet 2>/dev/null; then
    echo -e "Creating Artifact Registry repository..."
    gcloud artifacts repositories create ${ARTIFACT_REGISTRY_REPO} \
        --repository-format=docker \
        --location=${REGION} \
        --description="CMEK POC container images"
fi
echo -e "${GREEN}✅ Artifact Registry ready${NC}"

# Configure Docker authentication
echo -e "${YELLOW}🔑 Configuring Docker authentication...${NC}"
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

# Build the container image
echo -e "${YELLOW}🏗️  Building container image...${NC}"
echo -e "This may take a few minutes..."
gcloud builds submit --tag ${IMAGE_NAME} .

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Container image built successfully${NC}"
else
    echo -e "${RED}❌ Failed to build container image${NC}"
    exit 1
fi

# Get Terraform outputs for environment variables
echo -e "${YELLOW}📋 Getting infrastructure details...${NC}"
cd ../terraform

# Check if terraform state exists
if [ ! -f "terraform.tfstate" ]; then
    echo -e "${RED}❌ Terraform state not found. Please run terraform apply first.${NC}"
    exit 1
fi

# Get outputs
KMS_KEY_ID=$(terraform output -raw kms_key_id 2>/dev/null || echo "")
BIGQUERY_DATASET=$(terraform output -raw bigquery_dataset_id 2>/dev/null || echo "")
STORAGE_BUCKET=$(terraform output -raw storage_bucket_name 2>/dev/null || echo "")

if [ -z "$KMS_KEY_ID" ] || [ -z "$BIGQUERY_DATASET" ] || [ -z "$STORAGE_BUCKET" ]; then
    echo -e "${RED}❌ Could not retrieve required Terraform outputs${NC}"
    echo -e "${YELLOW}Please ensure terraform apply has been run successfully${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Retrieved infrastructure details${NC}"
echo -e "   KMS Key: ${KMS_KEY_ID}"
echo -e "   Dataset: ${BIGQUERY_DATASET}"
echo -e "   Bucket: ${STORAGE_BUCKET}"

cd ../app

# Deploy to Cloud Run
echo -e "${YELLOW}🚀 Deploying to Cloud Run...${NC}"
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --platform managed \
    --region ${REGION} \
    --allow-unauthenticated \
    --set-env-vars PROJECT_ID=${PROJECT_ID} \
    --set-env-vars REGION=${REGION} \
    --set-env-vars KMS_KEY_ID=${KMS_KEY_ID} \
    --set-env-vars BIGQUERY_DATASET=${BIGQUERY_DATASET} \
    --set-env-vars STORAGE_BUCKET=${STORAGE_BUCKET} \
    --memory 4Gi \
    --cpu 2 \
    --timeout 3600 \
    --max-instances 5 \
    --concurrency 1000

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Deployment successful!${NC}"
    
    # Get service URL
    SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --region=${REGION} --format="value(status.url)")
    
    echo ""
    echo -e "${GREEN}🎉 CMEK POC Application Deployed Successfully!${NC}"
    echo -e "${GREEN}=============================================${NC}"
    echo ""
    echo -e "Service URL: ${BLUE}${SERVICE_URL}${NC}"
    echo ""
    echo -e "${YELLOW}📋 Available Endpoints:${NC}"
    echo -e "   Health Check: ${BLUE}${SERVICE_URL}/${NC}"
    echo -e "   Process Data: ${BLUE}${SERVICE_URL}/process${NC} (POST)"
    echo -e "   Validate CMEK: ${BLUE}${SERVICE_URL}/validate${NC} (GET)"
    echo -e "   Get Metrics: ${BLUE}${SERVICE_URL}/metrics${NC} (GET)"
    echo ""
    echo -e "${YELLOW}🧪 Quick Test Commands:${NC}"
    echo -e "   Health Check: ${GREEN}curl ${SERVICE_URL}/${NC}"
    echo -e "   Validate CMEK: ${GREEN}curl ${SERVICE_URL}/validate${NC}"
    echo -e "   Process Data: ${GREEN}curl -X POST ${SERVICE_URL}/process -H 'Content-Type: application/json' -d '{\"action\": \"test_data_generation\"}'${NC}"
    echo ""
    echo -e "${BLUE}💡 The application is now ready to generate data for your LinkedIn post!${NC}"
    
else
    echo -e "${RED}❌ Deployment failed${NC}"
    exit 1
fi 