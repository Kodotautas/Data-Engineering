use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PerformanceMetric {
    pub metric_id: String,
    pub test_run_date: DateTime<Utc>,
    pub encryption_type: String,
    pub operation_type: String,
    pub execution_time_ms: u64,
    pub rows_processed: usize,
    pub data_size_bytes: usize,
}

pub struct PerformanceMeasurement {
    pub id: Uuid,
    pub operation_type: String,
    pub encryption_type: String,
    pub start_time: Instant,
}

impl PerformanceMeasurement {
    pub fn new(operation_type: &str, encryption_type: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            operation_type: operation_type.to_string(),
            encryption_type: encryption_type.to_string(),
            start_time: Instant::now(),
        }
    }
}

pub struct PerformanceMonitor {
    metrics: Vec<PerformanceMetric>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    pub fn start_measurement(&self, operation_type: &str, encryption_type: &str) -> PerformanceMeasurement {
        info!("Starting performance measurement for {} with {}", operation_type, encryption_type);
        PerformanceMeasurement::new(operation_type, encryption_type)
    }

    pub fn complete_measurement(
        &self,
        measurement: PerformanceMeasurement,
        rows_processed: usize,
        data_size_bytes: usize,
    ) {
        let execution_time = measurement.start_time.elapsed();
        let execution_time_ms = execution_time.as_millis() as u64;

        info!(
            "Completed measurement {} for {} in {}ms, processed {} rows, {} bytes",
            measurement.id,
            measurement.operation_type,
            execution_time_ms,
            rows_processed,
            data_size_bytes
        );

        // In a real implementation, this would save to the metrics vector
        // For now, we just log the completion
    }

    pub fn get_metrics(&self) -> &[PerformanceMetric] {
        &self.metrics
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
} 