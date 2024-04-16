use datafusion::prelude::*;
use tokio;
use std::time::Instant;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let now = Instant::now();
    let fusion = SessionContext::new();
    // get current working directory
    let current_dir = std::env::current_dir()?;
    
    // Join with the 'data' directory
    let data_dir = current_dir.join("data/*.csv");
    println!("Reading data from: {:?}", data_dir);

    let df = fusion.read_csv(data_dir.to_str().unwrap(), CsvReadOptions::new()).await?;
  
    let df = df.aggregate(vec![col("Time")], vec![count(col("Amount"))])?;

    df.show_limit(100).await?;
    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}