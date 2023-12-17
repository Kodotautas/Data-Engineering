use std::fs;
use std::env;
use std::fs::File;
use std::io::Write;
use std::time::Instant;
use tokio::runtime::Runtime;
use polars::prelude::*;
struct FileHandler;

impl FileHandler {
    fn read_csv_with_polars(file_name: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let df = CsvReader::from_path(file_name)?.finish()?;
        
        Ok(df)
    }

    async fn read_from_bigquery() -> Result<(), Box<dyn std::error::Error>> {
        let gcp_sa_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .map_err(|_| "Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set")?;
        let _client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?;
        
        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_file_name = "/home/vytautas/Desktop/chess_games.csv";

    let start = Instant::now();
    let _df = FileHandler::read_csv_with_polars(source_file_name)?;
    let duration = start.elapsed();

    let metadata = fs::metadata(source_file_name)?;
    let file_size = metadata.len();

    let mut file = File::create("src/times.txt")?;
    write!(file, "Time elapsed with Rust Polars: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    let start = Instant::now();
    let rt = Runtime::new()?;
    rt.block_on(FileHandler::read_from_bigquery())?;
    let duration = start.elapsed();

    write!(file, "Time elapsed with Rust read from BigQuery: {} seconds to upload {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    Ok(())
}