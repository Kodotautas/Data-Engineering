#!/usr/bin/env python3
"""
CMEK POC - Cloud Run Data Processor
Demonstrates Customer-Managed Encryption Keys with BigQuery and Cloud Storage
"""

import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

from flask import Flask, request, jsonify
from google.cloud import bigquery, storage, kms
from google.api_core import exceptions
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Environment variables from Terraform
PROJECT_ID = os.getenv('PROJECT_ID', 'vl-data-learn')
REGION = os.getenv('REGION', 'europe-west1')
KMS_KEY_ID = os.getenv('KMS_KEY_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'cmek_poc_dataset')
STORAGE_BUCKET = os.getenv('STORAGE_BUCKET')

# Initialize Google Cloud clients
bigquery_client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client(project=PROJECT_ID)
kms_client = kms.KeyManagementServiceClient()


class CMEKValidator:
    """Validates CMEK encryption for BigQuery and Cloud Storage"""
    
    def __init__(self):
        self.bq_client = bigquery_client
        self.storage_client = storage_client
        self.kms_client = kms_client
    
    def validate_bigquery_encryption(self, dataset_id: str) -> Dict[str, Any]:
        """Validate BigQuery dataset CMEK encryption"""
        try:
            dataset = self.bq_client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
            
            encryption_config = dataset.default_encryption_configuration
            if encryption_config and encryption_config.kms_key_name:
                return {
                    "encrypted": True,
                    "kms_key": encryption_config.kms_key_name,
                    "encryption_type": "CMEK",
                    "dataset": dataset_id
                }
            else:
                return {
                    "encrypted": True,
                    "encryption_type": "Google-managed",
                    "dataset": dataset_id
                }
                
        except Exception as e:
            logger.error(f"Error validating BigQuery encryption: {e}")
            return {"encrypted": False, "error": str(e)}
    
    def validate_storage_encryption(self, bucket_name: str) -> Dict[str, Any]:
        """Validate Cloud Storage bucket CMEK encryption"""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.reload()
            
            if bucket.default_kms_key_name:
                return {
                    "encrypted": True,
                    "kms_key": bucket.default_kms_key_name,
                    "encryption_type": "CMEK",
                    "bucket": bucket_name
                }
            else:
                return {
                    "encrypted": True,
                    "encryption_type": "Google-managed",
                    "bucket": bucket_name
                }
                
        except Exception as e:
            logger.error(f"Error validating Storage encryption: {e}")
            return {"encrypted": False, "error": str(e)}
    
    def validate_kms_key_access(self, key_id: str) -> Dict[str, Any]:
        """Validate KMS key access and rotation"""
        try:
            # Get key information
            key = self.kms_client.get_crypto_key(name=key_id)
            
            # Get key versions
            versions = list(self.kms_client.list_crypto_key_versions(parent=key_id))
            
            return {
                "key_accessible": True,
                "key_name": key.name,
                "purpose": key.purpose.name,
                "rotation_period": str(key.rotation_period),
                "total_versions": len(versions),
                "primary_version": key.primary.name if key.primary else None,
                "created_time": str(key.create_time) if key.create_time else None
            }
            
        except Exception as e:
            logger.error(f"Error validating KMS key: {e}")
            return {"key_accessible": False, "error": str(e)}


class DataGenerator:
    """Generates sample data for testing CMEK performance"""
    
    @staticmethod
    def generate_customers(num_records: int = 1000000000) -> pd.DataFrame:
        """Generate sample customer data with PII"""
        np.random.seed(42)  # For reproducible data
        
        customers = []
        for i in range(num_records):
            customer = {
                'customer_id': f"CUST_{i+1:06d}",
                'email': f"user_{i+1}@example{np.random.randint(1, 10)}.com",
                'full_name': f"Customer {i+1}",
                'phone_number': f"+1{np.random.randint(1000000000, 9999999999)}",
                'registration_date': datetime.now(timezone.utc),
                'country': np.random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'AU'])
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)
    
    @staticmethod
    def generate_transactions(num_records: int = 1000000000) -> pd.DataFrame:
        """Generate sample transaction data"""
        np.random.seed(42)
        
        transactions = []
        for i in range(num_records):
            transaction = {
                'transaction_id': f"TXN_{i+1:08d}",
                'customer_id': f"CUST_{np.random.randint(1, 1001):06d}",
                'amount': round(np.random.uniform(10.0, 2000.0), 2),
                'currency': np.random.choice(['USD', 'EUR', 'GBP', 'CAD']),
                'transaction_date': datetime.now(timezone.utc),
                'merchant_name': f"Merchant {np.random.randint(1, 100)}",
                'category': np.random.choice(['Food', 'Transport', 'Shopping', 'Entertainment', 'Healthcare'])
            }
            transactions.append(transaction)
        
        return pd.DataFrame(transactions)


class PerformanceMonitor:
    """Monitors and records performance metrics for CMEK operations"""
    
    def __init__(self):
        self.bq_client = bigquery_client
        self.metrics = []
    
    def measure_operation(self, operation_type: str, encryption_type: str):
        """Context manager to measure operation performance"""
        return PerformanceMeasurement(self, operation_type, encryption_type)
    
    def record_metric(self, metric_data: Dict[str, Any]):
        """Record a performance metric"""
        metric = {
            'metric_id': str(uuid.uuid4()),
            'test_run_date': datetime.now(timezone.utc),
            **metric_data
        }
        self.metrics.append(metric)
        logger.info(f"Recorded metric: {metric['operation_type']} - {metric['execution_time_ms']}ms")
    
    def save_metrics_to_bigquery(self) -> Dict[str, Any]:
        """Save collected metrics to BigQuery"""
        if not self.metrics:
            return {"status": "no_metrics", "saved_count": 0}
        
        try:
            table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.performance_metrics"
            table = self.bq_client.get_table(table_id)
            
            errors = self.bq_client.insert_rows_json(table, self.metrics)
            
            if errors:
                logger.error(f"Error inserting metrics: {errors}")
                return {"status": "error", "errors": errors}
            
            saved_count = len(self.metrics)
            self.metrics.clear()  # Clear after saving
            
            return {"status": "success", "saved_count": saved_count}
            
        except Exception as e:
            logger.error(f"Error saving metrics: {e}")
            return {"status": "error", "error": str(e)}


class PerformanceMeasurement:
    """Context manager for measuring operation performance"""
    
    def __init__(self, monitor: PerformanceMonitor, operation_type: str, encryption_type: str):
        self.monitor = monitor
        self.operation_type = operation_type
        self.encryption_type = encryption_type
        self.start_time = None
        self.rows_processed = 0
        self.data_size_bytes = 0
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is not None:
            execution_time_ms = int((time.time() - self.start_time) * 1000)
        else:
            execution_time_ms = 0
        
        self.monitor.record_metric({
            'encryption_type': self.encryption_type,
            'operation_type': self.operation_type,
            'execution_time_ms': execution_time_ms,
            'rows_processed': self.rows_processed,
            'data_size_bytes': self.data_size_bytes
        })
    
    def set_metrics(self, rows_processed: int, data_size_bytes: int = 0):
        """Set additional metrics for the operation"""
        self.rows_processed = rows_processed
        self.data_size_bytes = data_size_bytes


@app.route('/')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "cmek-poc-processor",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "project_id": PROJECT_ID,
            "region": REGION,
            "dataset": BIGQUERY_DATASET,
            "bucket": STORAGE_BUCKET
        }
    })


@app.route('/process', methods=['POST'])
def process_data():
    """Main data processing endpoint"""
    try:
        # Parse request
        request_data = request.get_json() or {}
        action = request_data.get('action', 'daily_processing')
        validate_encryption = request_data.get('validate_encryption', True)
        run_performance_test = request_data.get('run_performance_test', True)
        
        logger.info(f"Processing request: {action}")
        
        results = {
            "action": action,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "results": {}
        }
        
        # Initialize components
        validator = CMEKValidator()
        performance_monitor = PerformanceMonitor()
        
        # 1. Validate encryption setup
        if validate_encryption:
            logger.info("Validating encryption setup...")
            
            encryption_validation = {
                "bigquery": validator.validate_bigquery_encryption(BIGQUERY_DATASET),
                "storage": validator.validate_storage_encryption(STORAGE_BUCKET) if STORAGE_BUCKET else {"encrypted": False, "error": "STORAGE_BUCKET not configured"},
                "kms_key": validator.validate_kms_key_access(KMS_KEY_ID) if KMS_KEY_ID else {"key_accessible": False, "error": "KMS_KEY_ID not configured"}
            }
            
            results["results"]["encryption_validation"] = encryption_validation
        
        # 2. Generate and process sample data
        if action in ['daily_processing', 'test_data_generation']:
            logger.info("Generating sample data...")
            
            # Generate sample datasets
            customers_df = DataGenerator.generate_customers(1000)
            transactions_df = DataGenerator.generate_transactions(5000)
            
            # Upload to Cloud Storage with performance monitoring
            with performance_monitor.measure_operation("STORAGE_UPLOAD", "CMEK") as measurement:
                bucket = storage_client.bucket(STORAGE_BUCKET)
                
                # Upload customers data
                customers_blob = bucket.blob("sample-data/customers.csv")
                customers_csv = customers_df.to_csv(index=False)
                customers_blob.upload_from_string(customers_csv, content_type='text/csv')
                
                # Upload transactions data
                transactions_blob = bucket.blob("sample-data/transactions.csv")
                transactions_csv = transactions_df.to_csv(index=False)
                transactions_blob.upload_from_string(transactions_csv, content_type='text/csv')
                
                measurement.set_metrics(
                    rows_processed=len(customers_df) + len(transactions_df),
                    data_size_bytes=len(customers_csv.encode()) + len(transactions_csv.encode())
                )
            
            # Load to BigQuery with performance monitoring
            with performance_monitor.measure_operation("BIGQUERY_LOAD", "CMEK") as measurement:
                # Load customers
                customers_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.customers"
                customers_job = bigquery_client.load_table_from_dataframe(
                    customers_df, customers_table_id,
                    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                )
                customers_job.result()
                
                # Load transactions
                transactions_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.transactions"
                transactions_job = bigquery_client.load_table_from_dataframe(
                    transactions_df, transactions_table_id,
                    job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                )
                transactions_job.result()
                
                measurement.set_metrics(
                    rows_processed=len(customers_df) + len(transactions_df)
                )
            
            results["results"]["data_processing"] = {
                "customers_uploaded": len(customers_df),
                "transactions_uploaded": len(transactions_df),
                "storage_files": [
                    f"gs://{STORAGE_BUCKET}/sample-data/customers.csv",
                    f"gs://{STORAGE_BUCKET}/sample-data/transactions.csv"
                ]
            }
        
        # 3. Run performance tests
        if run_performance_test:
            logger.info("Running performance tests...")
            
            # Query performance test
            with performance_monitor.measure_operation("BIGQUERY_QUERY", "CMEK") as measurement:
                query = f"""
                SELECT c.country, COUNT(*) as customer_count, AVG(t.amount) as avg_transaction
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.customers` c
                JOIN `{PROJECT_ID}.{BIGQUERY_DATASET}.transactions` t ON c.customer_id = t.customer_id
                GROUP BY c.country
                ORDER BY customer_count DESC
                """
                
                query_job = bigquery_client.query(query)
                query_results = list(query_job.result())
                
                measurement.set_metrics(rows_processed=len(query_results))
            
            results["results"]["performance_test"] = {
                "query_completed": True,
                "result_rows": len(query_results),
                "sample_results": [dict(row) for row in query_results[:3]]
            }
        
        # 4. Save performance metrics
        metrics_result = performance_monitor.save_metrics_to_bigquery()
        results["results"]["metrics_saved"] = metrics_result
        
        logger.info(f"Processing completed successfully")
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Error processing request: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }), 500


@app.route('/validate', methods=['GET'])
def validate_encryption():
    """Endpoint to validate encryption setup"""
    try:
        validator = CMEKValidator()
        
        validation_results = {
            "bigquery": validator.validate_bigquery_encryption(BIGQUERY_DATASET),
            "storage": validator.validate_storage_encryption(STORAGE_BUCKET) if STORAGE_BUCKET else {"encrypted": False, "error": "STORAGE_BUCKET not configured"},
            "kms_key": validator.validate_kms_key_access(KMS_KEY_ID) if KMS_KEY_ID else {"key_accessible": False, "error": "KMS_KEY_ID not configured"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return jsonify(validation_results)
        
    except Exception as e:
        logger.error(f"Error validating encryption: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }), 500


@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Endpoint to retrieve performance metrics"""
    try:
        query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.performance_metrics`
        ORDER BY test_run_date DESC
        LIMIT 100
        """
        
        query_job = bigquery_client.query(query)
        results = [dict(row) for row in query_job.result()]
        
        return jsonify({
            "status": "success",
            "metrics_count": len(results),
            "metrics": results,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)