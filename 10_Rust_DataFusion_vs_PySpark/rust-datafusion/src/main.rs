use datafusion::prelude::*;
use tokio;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let start_time = Instant::now();
    let fusion = SessionContext::new();

    // Read all CSV files in the data folder
    let df = fusion.read_csv("data/*.csv", CsvReadOptions::new()).await?
        .aggregate(vec![col("rideable_type")], vec![count(col("start_lat"))])?;

    df.show().await?;
    println!("Elapsed: {:.2?}", start_time.elapsed());
    Ok(())
}