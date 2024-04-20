use datafusion::prelude::*;
use tokio;
use std::time::Instant;


fn main() {
    println!("DataFusion - NoSQL");
    no_sql().unwrap();
    // println!("DataFusion - SQL");
    // sql().unwrap();
}

#[tokio::main]
async fn no_sql() -> datafusion::error::Result<()> {
    let start_time = Instant::now();
    let fusion = SessionContext::new();

    // Read all CSV files in the data folder
    let df = fusion.read_csv("data/*.csv", CsvReadOptions::new()).await?
        .aggregate(vec![col("rideable_type")], vec![count(col("start_lat"))])?;

    df.show().await?;
    println!("Elapsed: {:.2?}", start_time.elapsed());
    Ok(())
}

#[tokio::main]
async fn sql() -> datafusion::error::Result<()> {
    let start_time = Instant::now();
    let fusion = SessionContext::new();

    // Read all CSV files in the data folder
    fusion.register_csv("rides", "data/*.csv", CsvReadOptions::new()).await?;

    // Not practical, but for test performance
    let df = fusion.sql("
        select 
            r.rideable_type,
            r2.member_casual,
            count(r3.ride_id) as total_rides,
            avg(r4.start_lat) as avg_start_lat,
            avg(r5.start_lng) as avg_start_lng
        from rides r
        left join rides r2 on r.ride_id = r2.ride_id
        left join rides r3 on r.ride_id = r3.ride_id
        left join rides r4 on r.ride_id = r4.ride_id
        left join rides r5 on r.ride_id = r5.ride_id
        group by r.rideable_type, r2.member_casual
    ").await?;

    df.show().await?;
    println!("Elapsed: {:.2?}", start_time.elapsed());
    Ok(())
}
