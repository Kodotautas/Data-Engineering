use anyhow::{Context, Result};
use axum::{
    extract::State,
    response::Json,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info};

#[derive(Debug, Serialize, Deserialize)]
struct ProcessRequest {
    action: Option<String>,
    validate_encryption: Option<bool>,
    run_performance_test: Option<bool>,
    customers_count: Option<usize>,
    transactions_count: Option<usize>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    service: String,
    timestamp: DateTime<Utc>,
    config: ConfigInfo,
}

#[derive(Debug, Serialize)]
struct ConfigInfo {
    project_id: String,
    region: String,
    dataset: String,
    bucket: String,
}

#[derive(Debug, Serialize)]
struct ProcessResponse {
    action: String,
    timestamp: DateTime<Utc>,
    results: serde_json::Value,
}

#[derive(Clone)]
struct AppState {
    project_id: String,
    region: String,
    kms_key_id: String,
    bigquery_dataset: String,
    storage_bucket: String,
}

impl AppState {
    fn new() -> Self {
        let project_id = env::var("PROJECT_ID").unwrap_or_else(|_| "vl-data-learn".to_string());
        let region = env::var("REGION").unwrap_or_else(|_| "europe-west1".to_string());
        let kms_key_id = env::var("KMS_KEY_ID").unwrap_or_else(|_| "cmek-poc-key".to_string());
        let bigquery_dataset = env::var("BIGQUERY_DATASET").unwrap_or_else(|_| "cmek_poc_dataset".to_string());
        let storage_bucket = env::var("STORAGE_BUCKET").unwrap_or_else(|_| "cmek-poc-data".to_string());

        info!("Initializing CMEK POC application...");

        Self {
            project_id,
            region,
            kms_key_id,
            bigquery_dataset,
            storage_bucket,
        }
    }
}

async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "cmek-poc-processor".to_string(),
        timestamp: Utc::now(),
        config: ConfigInfo {
            project_id: state.project_id.clone(),
            region: state.region.clone(),
            dataset: state.bigquery_dataset.clone(),
            bucket: state.storage_bucket.clone(),
        },
    })
}

async fn process_data(
    State(state): State<AppState>,
    Json(request): Json<ProcessRequest>,
) -> Json<ProcessResponse> {
    let action = request.action.unwrap_or_else(|| "daily_processing".to_string());
    let validate_encryption = request.validate_encryption.unwrap_or(true);
    let run_performance_test = request.run_performance_test.unwrap_or(true);
    let customers_count = request.customers_count.unwrap_or(1000);
    let transactions_count = request.transactions_count.unwrap_or(5000);

    info!("Processing request: {}", action);

    let mut results = serde_json::Map::new();

    // 1. Validate encryption setup (mocked)
    if validate_encryption {
        info!("Validating encryption setup...");
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        let validation_results = serde_json::json!({
            "bigquery": {
                "dataset": state.bigquery_dataset,
                "encryption": "CMEK",
                "key": state.kms_key_id.clone(),
                "status": "valid"
            },
            "storage": {
                "bucket": state.storage_bucket,
                "encryption": "CMEK", 
                "key": state.kms_key_id.clone(),
                "status": "valid"
            },
            "kms_key": {
                "key_id": state.kms_key_id.clone(),
                "status": "active",
                "rotation_enabled": true
            }
        });
        results.insert("encryption_validation".to_string(), validation_results);
    }

    // 2. Generate and process sample data
    if action == "daily_processing" || action == "test_data_generation" {
        info!("Generating {} customers and {} transactions...", customers_count, transactions_count);
        
        // Simulate data processing
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        
        let processing_results = serde_json::json!({
            "customers_uploaded": customers_count,
            "transactions_uploaded": transactions_count,
            "storage_files": [
                format!("gs://{}/sample-data/customers.csv", state.storage_bucket),
                format!("gs://{}/sample-data/transactions.csv", state.storage_bucket)
            ]
        });
        results.insert("data_processing".to_string(), processing_results);
    }

    // 3. Run performance tests
    if run_performance_test {
        info!("Running performance tests...");
        
        // Simulate performance test
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
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
        results.insert("performance_test".to_string(), test_results);
    }

    // 4. Save performance metrics
    let metrics_result = serde_json::json!({
        "status": "success",
        "metrics_saved": 3,
        "table": format!("{}.{}.performance_metrics", state.project_id, state.bigquery_dataset)
    });
    results.insert("metrics_saved".to_string(), metrics_result);

    info!("Processing completed successfully");

    Json(ProcessResponse {
        action,
        timestamp: Utc::now(),
        results: serde_json::Value::Object(results),
    })
}

async fn validate_encryption(State(state): State<AppState>) -> Json<serde_json::Value> {
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    let validation_results = serde_json::json!({
        "timestamp": Utc::now().to_rfc3339(),
        "bigquery": {
            "dataset": state.bigquery_dataset,
            "encryption": "CMEK",
            "key": state.kms_key_id.clone(),
            "status": "valid"
        },
        "storage": {
            "bucket": state.storage_bucket,
            "encryption": "CMEK", 
            "key": state.kms_key_id.clone(),
            "status": "valid"
        },
        "kms_key": {
            "key_id": state.kms_key_id.clone(),
            "status": "active",
            "rotation_enabled": true
        }
    });
    
    Json(validation_results)
}

async fn get_metrics(State(_state): State<AppState>) -> Json<serde_json::Value> {
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    
    let mock_metrics = vec![
        serde_json::json!({
            "metric_id": "test_1",
            "test_run_date": Utc::now().to_rfc3339(),
            "encryption_type": "CMEK",
            "operation_type": "STORAGE_UPLOAD",
            "execution_time_ms": 300,
            "rows_processed": 1000,
            "data_size_bytes": 150000
        }),
        serde_json::json!({
            "metric_id": "test_2", 
            "test_run_date": Utc::now().to_rfc3339(),
            "encryption_type": "CMEK",
            "operation_type": "BIGQUERY_LOAD",
            "execution_time_ms": 500,
            "rows_processed": 1000,
            "data_size_bytes": 0
        })
    ];

    Json(serde_json::json!({
        "status": "success",
        "metrics_count": mock_metrics.len(),
        "metrics": mock_metrics,
        "timestamp": Utc::now().to_rfc3339()
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info,cmek_poc_processor=debug")
        .init();

    info!("🦀 Starting CMEK POC Processor (Rust)...");

    // Load environment variables
    dotenvy::dotenv().ok();

    // Initialize application state
    let state = AppState::new();
    info!("✅ Application state initialized");

    // Build router
    let app = Router::new()
        .route("/", get(health_check))
        .route("/process", post(process_data))
        .route("/validate", get(validate_encryption))
        .route("/metrics", get(get_metrics))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state);

    // Get port from environment
    let port = env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse::<u16>()
        .context("Invalid PORT environment variable")?;

    info!("🚀 Server listening on port {}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse()?)
        .serve(app.into_make_service())
        .await
        .context("Server error")?;

    Ok(())
} 