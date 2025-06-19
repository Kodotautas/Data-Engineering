# CMEK POC - Cloud Run Application

This Cloud Run application demonstrates **Customer-Managed Encryption Keys (CMEK)** with BigQuery and Cloud Storage, generating real performance metrics for data engineering analysis.

## 🎯 What This Application Does

### 🔐 **CMEK Validation**
- Validates BigQuery dataset encryption with customer-managed keys
- Verifies Cloud Storage bucket CMEK configuration  
- Checks KMS key accessibility and rotation settings
- Provides detailed encryption status reports

### 📊 **Data Processing Pipeline**
- Generates realistic sample datasets (customers + transactions)
- Uploads data to CMEK-encrypted Cloud Storage
- Loads data into CMEK-encrypted BigQuery tables
- Measures and records performance metrics

### 📈 **Performance Monitoring**
- Times all encryption operations (Storage upload, BigQuery load, queries)
- Records metrics: execution time, rows processed, data size
- Stores results in BigQuery for analysis
- Provides REST API for metric retrieval

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Cloud Run     │────▶│  Cloud Storage   │────▶│    BigQuery     │
│  (This App)     │    │     (CMEK)       │    │     (CMEK)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         ▼                        ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Sample Data   │    │   Encrypted      │    │   Performance   │
│   Generation    │    │   File Storage   │    │    Metrics      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 🚀 Quick Deployment

### Prerequisites
- ✅ Terraform infrastructure deployed (Phase 1 complete)
- ✅ Google Cloud CLI authenticated
- ✅ Docker and Cloud Build API enabled

### Deploy the Application

1. **Navigate to app directory**
   ```bash
   cd 15_topic_name/app
   ```

2. **Make deploy script executable**
   ```bash
   chmod +x deploy.sh
   ```

3. **Deploy the application**
   ```bash
   ./deploy.sh
   ```

The script will:
- Build the container image using Cloud Build
- Retrieve configuration from Terraform state
- Deploy to Cloud Run with proper environment variables
- Provide you with service URL and test commands

## 📡 API Endpoints

### `GET /` - Health Check
Returns service status and configuration.

```bash
curl https://your-service-url/
```

### `GET /validate` - Validate CMEK Setup
Checks encryption configuration across all services.

```bash
curl https://your-service-url/validate
```

**Example Response:**
```json
{
  "bigquery": {
    "encrypted": true,
    "encryption_type": "CMEK",
    "kms_key": "projects/vl-data-learn/locations/europe-west1/keyRings/cmek-poc-keyring/cryptoKeys/cmek-poc-key",
    "dataset": "cmek_poc_dataset"
  },
  "storage": {
    "encrypted": true,
    "encryption_type": "CMEK",
    "kms_key": "projects/vl-data-learn/locations/europe-west1/keyRings/cmek-poc-keyring/cryptoKeys/cmek-poc-key",
    "bucket": "cmek-poc-data-abc123"
  },
  "kms_key": {
    "key_accessible": true,
    "purpose": "ENCRYPT_DECRYPT",
    "rotation_period": "7776000s",
    "total_versions": 1
  }
}
```

### `POST /process` - Main Data Processing
Generates sample data, processes with CMEK, and records metrics.

```bash
curl -X POST https://your-service-url/process \
  -H "Content-Type: application/json" \
  -d '{
    "action": "daily_processing",
    "validate_encryption": true,
    "run_performance_test": true
  }'
```

**Parameters:**
- `action`: Type of processing (`daily_processing`, `test_data_generation`)
- `validate_encryption`: Whether to validate CMEK setup (default: true)
- `run_performance_test`: Whether to run performance benchmarks (default: true)

### `GET /metrics` - Retrieve Performance Metrics
Returns collected performance data for analysis.

```bash
curl https://your-service-url/metrics
```

## 📊 Sample Data Generated

### 👥 **Customers Table** (1,000 records)
- `customer_id`: Unique identifier
- `email`: Customer email (PII)
- `full_name`: Customer name (PII) 
- `phone_number`: Phone number (PII)
- `registration_date`: Account creation timestamp
- `country`: Customer country

### 💳 **Transactions Table** (5,000 records)
- `transaction_id`: Unique transaction ID
- `customer_id`: Foreign key to customers
- `amount`: Transaction amount
- `currency`: Transaction currency
- `transaction_date`: Transaction timestamp
- `merchant_name`: Merchant name
- `category`: Transaction category

### 📈 **Performance Metrics Table**
- `metric_id`: Unique metric identifier
- `test_run_date`: Test execution time
- `encryption_type`: CMEK or DEFAULT
- `operation_type`: STORAGE_UPLOAD, BIGQUERY_LOAD, BIGQUERY_QUERY
- `execution_time_ms`: Operation duration
- `rows_processed`: Number of rows processed
- `data_size_bytes`: Data size in bytes

## 🔬 Performance Testing

The application measures:

1. **Storage Upload Performance**: Time to upload CSV files to CMEK-encrypted bucket
2. **BigQuery Load Performance**: Time to load data into CMEK-encrypted tables
3. **Query Performance**: Time to execute analytics queries on encrypted data

All metrics are automatically stored for analysis and comparison.

## 🧪 Testing Your Deployment

After deployment, test with these commands:

```bash
# Get your service URL from deployment output
SERVICE_URL="https://your-service-url"

# 1. Health Check
curl $SERVICE_URL/

# 2. Validate CMEK Configuration
curl $SERVICE_URL/validate

# 3. Process Test Data
curl -X POST $SERVICE_URL/process \
  -H "Content-Type: application/json" \
  -d '{"action": "test_data_generation"}'

# 4. Get Performance Metrics
curl $SERVICE_URL/metrics
```

## 📝 Generated Metrics

- **Encryption validation results** (CMEK vs default)
- **Performance benchmarks** (upload times, query times)
- **Data processing metrics** (rows processed, file sizes)
- **Security verification** (key rotation, access controls)

## 🔧 Customization

### Modify Sample Data Size
Edit `main.py` and change the default values:

```python
customers_df = DataGenerator.generate_customers(5000)  # Increase size
transactions_df = DataGenerator.generate_transactions(25000)  # Increase size
```

### Add Custom Metrics
Extend the `PerformanceMonitor` class to track additional operations.

### Change Processing Schedule
The Cloud Scheduler is configured in Terraform - modify `terraform/cloud_run.tf` to change the schedule.

## 🐛 Troubleshooting

### Common Issues

**Build Failures**
```bash
# Check Cloud Build API is enabled
gcloud services enable cloudbuild.googleapis.com
```

**Permission Errors**
```bash
# Verify service account has proper roles
gcloud projects get-iam-policy vl-data-learn
```

**Terraform State Not Found**
```bash
# Ensure Phase 1 (Terraform) is completed
cd ../terraform && terraform apply
```

## 📚 Next Steps

1. **Run the application** to generate sample data
2. **Collect performance metrics** for your analysis
3. **Create visualizations** of the encryption performance

