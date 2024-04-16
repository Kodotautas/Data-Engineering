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

    // create a Dataframe
    let df = fusion.read_csv(data_dir.to_str().unwrap(), CsvReadOptions::new()).await?;

    // create a plan
    let df = df.filter(col("Time").lt_eq(col("Amount")))?
    .aggregate(vec![col("Time")], vec![sum(col("Amount"))])?
    .limit(0, Some(100))?;

    // execute the plan
    let results = df.collect().await?;
    println!("Results: {:?}", results);

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);
    Ok(())
}