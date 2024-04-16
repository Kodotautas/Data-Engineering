use datafusion::prelude::*;
use tokio;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let now = Instant::now();
    let fusion = SessionContext::new();
    let data_dir = std::env::current_dir()?.join("data/*.csv");

    let df = fusion.read_csv(data_dir.to_str().unwrap(), CsvReadOptions::new()).await?
        .filter(col("Time").lt_eq(col("Amount")))?
        .aggregate(vec![col("Time")], vec![sum(col("Amount"))])?
        .limit(0, Some(100))?;

    println!("Results: {:?}", df.collect().await?);
    println!("Elapsed: {:.2?}", now.elapsed());
    Ok(())
}