#!/bin/bash

# CMEK POC - One-Step Deployment Script (Rust Version)
# This script automates the complete deployment of the CMEK proof-of-concept
# including infrastructure setup, Rust application deployment, and testing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to validate email format
validate_email() {
    local email=$1
    if [[ ! $email =~ ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$ ]]; then
        return 1
    fi
    return 0
}

# Function to validate project ID
validate_project_id() {
    local project_id=$1
    if [[ ! $project_id =~ ^[a-z][a-z0-9-]{4,28}[a-z0-9]$ ]]; then
        return 1
    fi
    return 0
}

# Function to check GCP authentication
check_gcp_auth() {
    print_status "Checking GCP authentication..."
    
    if ! command_exists gcloud; then
        print_error "Google Cloud CLI (gcloud) is not installed. Please install it first."
        print_status "Installation guide: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
    
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        print_error "Not authenticated with Google Cloud. Please run 'gcloud auth login' first."
        exit 1
    fi
    
    print_success "GCP authentication verified"
}

# Function to check required tools
check_requirements() {
    print_status "Checking required tools..."
    
    local missing_tools=()
    
    if ! command_exists terraform; then
        missing_tools+=("terraform")
    fi
    
    if ! command_exists gcloud; then
        missing_tools+=("gcloud")
    fi
    
    # Make Docker optional since Cloud Build handles container building
    if ! command_exists docker; then
        print_warning "Docker not found. This is optional - Cloud Build will handle container building."
        print_status "If you want to build containers locally, install Docker with: sudo apt install docker.io"
    fi
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        print_status "Please install the missing tools and try again."
        exit 1
    fi
    
    print_success "All required tools are available"
}

# Function to get user input
get_user_input() {
    print_status "Setting up CMEK POC deployment..."
    echo
    
    # Get project ID
    while true; do
        read -p "Enter your GCP Project ID: " PROJECT_ID
        if validate_project_id "$PROJECT_ID"; then
            break
        else
            print_error "Invalid project ID format. Project ID must be 6-30 characters long, contain only lowercase letters, numbers, and hyphens, and start with a letter."
        fi
    done
    
    # Get user email
    while true; do
        read -p "Enter your email address (for BigQuery dataset ownership): " USER_EMAIL
        if validate_email "$USER_EMAIL"; then
            break
        else
            print_error "Invalid email format. Please enter a valid email address."
        fi
    done
    
    # Get region
    echo
    print_status "Available regions:"
    echo "1. us-central1 (Iowa)"
    echo "2. us-east1 (South Carolina)"
    echo "3. europe-west1 (Belgium)"
    echo "4. asia-east1 (Taiwan)"
    echo "5. Custom"
    
    while true; do
        read -p "Select region (1-5): " region_choice
        case $region_choice in
            1) REGION="us-central1"; break ;;
            2) REGION="us-east1"; break ;;
            3) REGION="europe-west1"; break ;;
            4) REGION="asia-east1"; break ;;
            5) 
                read -p "Enter custom region: " REGION
                if [[ -n "$REGION" ]]; then
                    break
                else
                    print_error "Region cannot be empty."
                fi
                ;;
            *) print_error "Invalid choice. Please select 1-5." ;;
        esac
    done
    
    # Confirm deployment
    echo
    print_warning "Deployment Summary:"
    echo "  Project ID: $PROJECT_ID"
    echo "  User Email: $USER_EMAIL"
    echo "  Region: $REGION"
    echo "  Environment: demo"
    echo
    print_warning "This will create the following resources:"
    echo "  - Cloud KMS key ring and encryption key"
    echo "  - BigQuery dataset with CMEK encryption"
    echo "  - Cloud Storage bucket with CMEK encryption"
    echo "  - Cloud Run service for data processing (Rust)"
    echo "  - Cloud Scheduler job for automation"
    echo "  - IAM service accounts and permissions"
    echo
    
    while true; do
        read -p "Do you want to proceed with the deployment? (y/N): " confirm
        case $confirm in
            [Yy]* ) break ;;
            [Nn]* ) 
                print_status "Deployment cancelled."
                exit 0
                ;;
            * ) print_error "Please answer yes (y) or no (n)." ;;
        esac
    done
}

# Function to setup Terraform
setup_terraform() {
    print_status "Setting up Terraform configuration..."
    
    cd terraform
    
    # Create terraform.tfvars
    cat > terraform.tfvars << EOF
project_id = "$PROJECT_ID"
dataset_owner_email = "$USER_EMAIL"
region = "$REGION"
environment = "demo"
key_rotation_period = "7776000"
enable_monitoring = true
EOF
    
    print_success "Terraform configuration created"
    
    # Initialize Terraform
    print_status "Initializing Terraform..."
    terraform init -input=false
    
    print_success "Terraform initialized"
}

# Function to deploy infrastructure
deploy_infrastructure() {
    print_status "Deploying infrastructure with Terraform..."
    
    # Plan deployment
    print_status "Planning Terraform deployment..."
    terraform plan -out=tfplan -input=false
    
    # Apply deployment
    print_status "Applying Terraform configuration..."
    terraform apply -input=false tfplan
    
    # Get outputs
    print_status "Getting deployment outputs..."
    CLOUD_RUN_URL=$(terraform output -raw cloud_run_url)
    STORAGE_BUCKET=$(terraform output -raw storage_bucket_name)
    DATASET_ID=$(terraform output -raw bigquery_dataset_id)
    KMS_KEY_ID=$(terraform output -raw kms_key_id)
    
    print_success "Infrastructure deployed successfully"
    
    # Save outputs for later use
    cat > ../deployment_outputs.env << EOF
CLOUD_RUN_URL=$CLOUD_RUN_URL
STORAGE_BUCKET=$STORAGE_BUCKET
DATASET_ID=$DATASET_ID
KMS_KEY_ID=$KMS_KEY_ID
PROJECT_ID=$PROJECT_ID
REGION=$REGION
EOF
    
    cd ..
}

# Function to deploy application
deploy_application() {
    print_status "Deploying Rust Cloud Run application..."
    
    cd rust-app
    
    # Deploy Rust application
    print_status "Building and deploying Rust CMEK POC..."
    gcloud run deploy cmek-poc-processor \
        --source . \
        --platform managed \
        --region "$REGION" \
        --allow-unauthenticated \
        --set-env-vars="PROJECT_ID=$PROJECT_ID,REGION=$REGION,BIGQUERY_DATASET=${DATASET_ID:-cmek_poc_dataset},STORAGE_BUCKET=$STORAGE_BUCKET,KMS_KEY_ID=$KMS_KEY_ID" \
        --memory=4Gi \
        --cpu=2 \
        --timeout=3600 \
        --max-instances=10 \
        --port=8080
    
    cd ..
    
    print_success "Rust application deployed successfully"
}

# Function to test deployment
test_deployment() {
    print_status "Testing CMEK deployment..."
    
    # Load deployment outputs
    source deployment_outputs.env
    
    # Wait for Cloud Run to be ready
    print_status "Waiting for Cloud Run service to be ready..."
    sleep 30
    
    # Test health check
    print_status "Testing health check..."
    if curl -s "$CLOUD_RUN_URL/" | grep -q "healthy"; then
        print_success "Health check passed"
    else
        print_error "Health check failed"
        return 1
    fi
    
    # Test CMEK validation
    print_status "Testing CMEK validation..."
    if curl -s "$CLOUD_RUN_URL/validate" | grep -q "CMEK"; then
        print_success "CMEK validation passed"
    else
        print_error "CMEK validation failed"
        return 1
    fi
    
    # Test data processing
    print_status "Testing data processing..."
    PROCESS_RESPONSE=$(curl -s -X POST "$CLOUD_RUN_URL/process" \
        -H "Content-Type: application/json" \
        -d '{
            "action": "test_data_generation",
            "validate_encryption": true,
            "run_performance_test": true
        }')
    
    if echo "$PROCESS_RESPONSE" | grep -q "customers_uploaded"; then
        print_success "Data processing test passed"
    else
        print_error "Data processing test failed"
        echo "Response: $PROCESS_RESPONSE"
        return 1
    fi
    
    print_success "All tests passed!"
}

# Function to display results
display_results() {
    print_status "Deployment completed successfully!"
    echo
    
    # Load deployment outputs
    source deployment_outputs.env
    
    print_success "CMEK POC is now running at: $CLOUD_RUN_URL"
    echo
    
    print_status "Quick Test Commands:"
    echo "  Health Check: curl $CLOUD_RUN_URL/"
    echo "  CMEK Validation: curl $CLOUD_RUN_URL/validate"
    echo "  Process Data: curl -X POST $CLOUD_RUN_URL/process -H 'Content-Type: application/json' -d '{\"action\": \"test_data_generation\"}'"
    echo "  Get Metrics: curl $CLOUD_RUN_URL/metrics"
    echo
    
    print_status "BigQuery Tables (encrypted with CMEK):"
    echo "  Dataset: $PROJECT_ID.$DATASET_ID"
    echo "  Tables: customers, transactions, performance_metrics"
    echo
    
    print_status "Cloud Storage (encrypted with CMEK):"
    echo "  Bucket: gs://$STORAGE_BUCKET"
    echo
    
    print_status "KMS Key:"
    echo "  Key ID: $KMS_KEY_ID"
    echo
    
    print_warning "To clean up all resources, run:"
    echo "  cd terraform && terraform destroy"
    echo
    
    print_success "🦀 CMEK POC (Rust) deployment completed successfully!"
}

# Function to handle errors
handle_error() {
    print_error "Deployment failed at step: $1"
    print_status "You can try to fix the issue and run the script again."
    print_status "To clean up partial resources, run: cd terraform && terraform destroy"
    exit 1
}

# Main execution
main() {
    echo "🦀 CMEK POC - One-Step Deployment (Rust)"
    echo "========================================"
    echo
    
    # Check requirements
    check_requirements
    
    # Check GCP authentication
    check_gcp_auth
    
    # Get user input
    get_user_input
    
    # Setup and deploy
    {
        setup_terraform || handle_error "Terraform setup"
        deploy_infrastructure || handle_error "Infrastructure deployment"
        deploy_application || handle_error "Application deployment"
        test_deployment || handle_error "Testing"
        display_results
    }
}

# Run main function
main "$@" 