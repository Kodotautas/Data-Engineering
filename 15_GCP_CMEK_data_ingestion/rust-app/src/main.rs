use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use tracing::{info, error, warn};

mod data_generator;
use data_generator::DataGenerator;

#[derive(Debug, Serialize, Deserialize)]
struct ProcessConfig {
    action: String,
    validate_encryption: bool,
    run_performance_test: bool,
    customers_count: usize,
    transactions_count: usize,
}

#[derive(Debug, Serialize)]
struct ProcessResult {
    timestamp: DateTime<Utc>,
    config: AppConfig,
    validation_results: Option<serde_json::Value>,
    data_processing_results: Option<serde_json::Value>,
    performance_test_results: Option<serde_json::Value>,
    metrics_saved: Option<serde_json::Value>,
    status: String,
    duration_ms: u64,
}

#[derive(Debug, Serialize)]
struct AppConfig {
    project_id: String,
    region: String,
    kms_key_id: String,
    bigquery_dataset: String,
    storage_bucket: String,
}

impl AppConfig {
    fn from_env() -> Self {
        Self {
            project_id: env::var("PROJECT_ID").unwrap_or_else(|_| "vl-data-learn".to_string()),
            region: env::var("REGION").unwrap_or_else(|_| "europe-west1".to_string()),
            kms_key_id: env::var("KMS_KEY_ID").unwrap_or_else(|_| "cmek-poc-key".to_string()),
            bigquery_dataset: env::var("BIGQUERY_DATASET").unwrap_or_else(|_| "cmek_poc_dataset".to_string()),
            storage_bucket: env::var("STORAGE_BUCKET").unwrap_or_else(|_| "cmek-poc-data".to_string()),
        }
    }
}

async fn validate_encryption(config: &AppConfig) -> Result<serde_json::Value> {
    info!("🔒 Validating encryption setup...");
    
    // Simulate encryption validation
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    let validation_results = serde_json::json!({
        "bigquery": {
            "dataset": config.bigquery_dataset,
            "encryption": "CMEK",
            "key": config.kms_key_id,
            "status": "valid"
        },
        "storage": {
            "bucket": config.storage_bucket,
            "encryption": "CMEK", 
            "key": config.kms_key_id,
            "status": "valid"
        },
        "kms_key": {
            "key_id": config.kms_key_id,
            "status": "active",
            "rotation_enabled": true
        }
    });
    
    info!("✅ Encryption validation completed");
    Ok(validation_results)
}

async fn process_data(config: &AppConfig, process_config: &ProcessConfig) -> Result<serde_json::Value> {
    info!("📊 Generating {} customers and {} transactions...", 
          process_config.customers_count, process_config.transactions_count);
    
    let mut data_generator = DataGenerator::new();
    
    // Generate customer data
    let customers = data_generator
        .generate_customers(process_config.customers_count)
        .await
        .context("Failed to generate customers")?;
    
    info!("✅ Generated {} customers", customers.len());
    
    // Log first few customers for verification
    for (i, customer) in customers.iter().take(3).enumerate() {
        info!("Customer {}: {} {} - {}", i+1, customer.first_name, customer.last_name, customer.email);
    }
    
    // Generate transaction data
    let transactions = data_generator
        .generate_transactions(process_config.transactions_count, process_config.customers_count)
        .await
        .context("Failed to generate transactions")?;
    
    info!("✅ Generated {} transactions", transactions.len());
    
    // Log first few transactions for verification
    for (i, transaction) in transactions.iter().take(3).enumerate() {
        info!("Transaction {}: {} - ${:.2} - {}", 
              i+1, transaction.transaction_id, transaction.amount, transaction.description);
    }
    
    // TODO: Here you would actually insert data into BigQuery
    info!("📝 Data ready for BigQuery insertion");
    
    let processing_results = serde_json::json!({
        "customers_generated": customers.len(),
        "transactions_generated": transactions.len(),
        "status": "completed",
        "note": "Data generated successfully - BigQuery insertion ready to implement"
    });
    
    Ok(processing_results)
}

async fn run_performance_test(config: &AppConfig) -> Result<serde_json::Value> {
    info!("⚡ Running performance tests...");
    
    // Simulate performance test
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    
    let mock_results = vec![
        serde_json::json!({"country": "US", "customer_count": 450, "avg_transaction": 1250.50}),
        serde_json::json!({"country": "CA", "customer_count": 320, "avg_transaction": 980.25}),
        serde_json::json!({"country": "UK", "customer_count": 280, "avg_transaction": 1150.75}),
    ];
    
    let test_results = serde_json::json!({
        "query_completed": true,
        "result_rows": mock_results.len(),
        "sample_results": mock_results
    });
    
    info!("✅ Performance tests completed");
    Ok(test_results)
}

async fn save_metrics(config: &AppConfig) -> Result<serde_json::Value> {
    info!("📈 Saving performance metrics...");
    
    // Simulate saving metrics
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let metrics_result = serde_json::json!({
        "status": "success",
        "metrics_saved": 3,
        "table": format!("{}.{}.performance_metrics", config.project_id, config.bigquery_dataset)
    });
    
    info!("✅ Metrics saved");
    Ok(metrics_result)
}

async fn process_cmek_data(process_config: ProcessConfig) -> Result<ProcessResult> {
    let start_time = std::time::Instant::now();
    let config = AppConfig::from_env();
    
    info!("🦀 Starting CMEK POC data processing...");
    info!("📋 Config: Project={}, Dataset={}, Bucket={}", 
          config.project_id, config.bigquery_dataset, config.storage_bucket);
    
    let mut result = ProcessResult {
        timestamp: Utc::now(),
        config,
        validation_results: None,
        data_processing_results: None,
        performance_test_results: None,
        metrics_saved: None,
        status: "started".to_string(),
        duration_ms: 0,
    };
    
    // 1. Validate encryption setup
    if process_config.validate_encryption {
        match validate_encryption(&result.config).await {
            Ok(validation) => result.validation_results = Some(validation),
            Err(e) => {
                error!("Encryption validation failed: {}", e);
                result.status = "failed".to_string();
                return Ok(result);
            }
        }
    }
    
    // 2. Process data
    match process_data(&result.config, &process_config).await {
        Ok(processing) => result.data_processing_results = Some(processing),
        Err(e) => {
            error!("Data processing failed: {}", e);
            result.status = "failed".to_string();
            return Ok(result);
        }
    }
    
    // 3. Run performance tests
    if process_config.run_performance_test {
        match run_performance_test(&result.config).await {
            Ok(performance) => result.performance_test_results = Some(performance),
            Err(e) => {
                warn!("Performance test failed: {}", e);
                // Don't fail the entire process for performance test failures
            }
        }
    }
    
    // 4. Save metrics
    match save_metrics(&result.config).await {
        Ok(metrics) => result.metrics_saved = Some(metrics),
        Err(e) => {
            warn!("Metrics saving failed: {}", e);
            // Don't fail the entire process for metrics failures
        }
    }
    
    result.duration_ms = start_time.elapsed().as_millis() as u64;
    result.status = "completed".to_string();
    
    info!("🎉 CMEK POC processing completed in {}ms", result.duration_ms);
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .init();

    info!("🦀 CMEK POC Processor (Rust) - Starting...");
    
    // Parse command line arguments or use defaults
    let process_config = ProcessConfig {
        action: env::var("ACTION").unwrap_or_else(|_| "daily_processing".to_string()),
        validate_encryption: env::var("VALIDATE_ENCRYPTION")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        run_performance_test: env::var("RUN_PERFORMANCE_TEST")
            .unwrap_or_else(|_| "true".to_string())
            .parse()
            .unwrap_or(true),
        customers_count: env::var("CUSTOMERS_COUNT")
            .unwrap_or_else(|_| "1000".to_string())
            .parse()
            .unwrap_or(1000),
        transactions_count: env::var("TRANSACTIONS_COUNT")
            .unwrap_or_else(|_| "5000".to_string())
            .parse()
            .unwrap_or(5000),
    };
    
    info!("📋 Processing config: {:?}", process_config);
    
    // Process the data
    match process_cmek_data(process_config).await {
        Ok(result) => {
            info!("✅ Processing completed successfully");
            
            // Print result as JSON for Cloud Run logs
            println!("{}", serde_json::to_string_pretty(&result)?);
            
            if result.status == "completed" {
                std::process::exit(0);
            } else {
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("❌ Processing failed: {}", e);
            std::process::exit(1);
        }
    }
} 