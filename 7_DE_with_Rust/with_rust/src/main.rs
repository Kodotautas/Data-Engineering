use std::fs;
use std::env;
use std::io::Write;
use std::fs::File;
use std::time::Instant;
use tokio::runtime::Runtime;
use polars::prelude::*;
struct FileHandler;
use gcp_bigquery_client::model::query_request::QueryRequest;

impl FileHandler {
    fn read_csv_with_polars(file_name: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let df = CsvReader::from_path(file_name)?.finish()?;
        
        Ok(df)
    }

    async fn read_from_bigquery() -> Result<(), Box<dyn std::error::Error>> {
        let gcp_sa_key = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .map_err(|_| "Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set")?;
        let client = gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?;
        
        let query = "SELECT * FROM `data-engineering-with-rust.data_tests.chess_games` limit 13000";
        
        let rs = client
            .job()
            .query("data-engineering-with-rust", QueryRequest::new(query))
            .await?;
    
        // Open a new file in write mode, or create it if it doesn't exist
        let mut file = File::create("output.txt")?;
    
        if let Some(rows) = &rs.query_response().rows {
            for row in rows {
                if let Some(columns) = &row.columns {
                    for cell in columns {
                        if let Some(value) = &cell.value {
                            // Write the value to the file
                            write!(file, "{} ", value)?;
                        }
                    }
                    // Write a newline at the end of each row
                    writeln!(file)?;
                }
            }
        }
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

    write!(file, "Time elapsed to read and write to file from BigQuery: {} seconds to read table and wtite it to .txt file.\n",
        duration.as_secs_f64())?;

    Ok(())
}