use std::fs;
use std::env;
use std::io::Write;
use std::fs::File;
use std::time::Instant;
use std::process::Command;
use std::error::Error;
use tokio::runtime::Runtime;
use polars::prelude::*;
struct FileHandler;
use gcp_bigquery_client::model::query_request::QueryRequest;
use std::borrow::Cow;
use std::io::Read;
use csv::ReaderBuilder;
use csv::StringRecord;
use arrow::csv as csv_arrow;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;


impl FileHandler {
    fn read_csv_with_polars(file_name: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let df = CsvReader::from_path(file_name)?.finish()?;
        
        Ok(df)
    }

    fn read_csv_with_polars_drop_nulls(file_name: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let df = CsvReader::from_path(file_name)?.finish()?;
    
        // Drop null values
        let df = df.drop_nulls::<&str>(None)?;
    
        Ok(df)
    }
    
    fn read_csv_and_filter_event_column(file_name: &str) -> Result<Vec<StringRecord>, Box<dyn Error>> {
        let mut rdr = ReaderBuilder::new().from_path(file_name)?;
        let headers = rdr.headers()?.clone();
    
        let mut records: Vec<StringRecord> = Vec::new();
        for result in rdr.records() {
            let record = result?;
            if let Some(event_index) = headers.iter().position(|h| h == "Event") {
                if let Some(event) = record.get(event_index) {
                    if event.trim() == "Blitz" {
                        records.push(record.clone());
                    }
                }
            }
        }
    
        Ok(records)
    }
    
    // fn read_csv_with_polars_convert_to_arrow(file_name: &str) -> Result<RecordBatch, Box<dyn Error>> {
    //     let df = CsvReader::from_path(file_name)?.finish()?;
    
    //     // Convert df to arrow format
    //     let arrow = df.to_arrow()?;

    //     Ok(arrow)
    // }

    fn csv_to_arrow(file_path: &str) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
        // Define the schema for the CSV file
        let schema = Schema::new(vec![
            Field::new("column1", DataType::Utf8, false),
            Field::new("column2", DataType::Int32, false),
            // Add more fields as per your CSV structure
        ]);
    
        // Create a CSV reader with the schema
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .infer_schema(Some(100))
            .from_reader(File::open(file_path)?);
    
        // Read the CSV file into a vector of RecordBatches
        let mut batches = Vec::new();
        while let Ok(Some(batch)) = reader.next() {
            batches.push(batch);
        }
    
        Ok(batches)
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
        let mut file = File::create("data_sample.txt")?;
    
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

    fn load_csv_to_bigquery(dataset: &str, table: &str, bucket: &str, file: &str) -> std::io::Result<()> {
        let output = Command::new("bq")
            .arg("load")
            .arg("--autodetect")
            .arg("--source_format=CSV")
            .arg(format!("{}.{}", dataset, table))
            .arg(format!("gs://{}/{}", bucket, file))
            .output()?;
    
        if !output.status.success() {
            eprintln!("Error: {}", String::from_utf8_lossy(&output.stderr));
        }
    
        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_file_name = "/home/vytautas/Desktop/chess_games.csv";

    // Read csv file with Polars
    // let start = Instant::now();
    // let _df = FileHandler::read_csv_with_polars(source_file_name)?;
    // let duration = start.elapsed();

    let metadata = fs::metadata(source_file_name)?;
    let file_size = metadata.len();

    // set apend file
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open("src/times.txt")?;

    // let mut file = File::create("src/times.txt")?;
    // write!(file, "Time elapsed with Rust Polars: {} seconds to read {} which size is {} bytes.\n", 
    //     duration.as_secs_f64(), source_file_name, file_size)?;

    // // Read csv file with Polars and drop null values
    // let start = Instant::now();
    // let _df = FileHandler::read_csv_with_polars_drop_nulls(source_file_name)?;
    // let duration = start.elapsed();

    // write!(file, "Time elapsed with Rust Polars and drop null values: {} seconds to read {} which size is {} bytes.\n", 
    //     duration.as_secs_f64(), source_file_name, file_size)?;


    // Read csv file with Polars and filter event column
    // let start = Instant::now();
    // let _df = FileHandler::read_csv_and_filter_event_column(source_file_name)?;
    // let duration = start.elapsed();

    // write!(file, "Time elapsed with Rust Polars and filter event column: {} seconds to read {} which size is {} bytes.\n", 
    //     duration.as_secs_f64(), source_file_name, file_size)?;

    // // Read csv file with Polars and convert to json
    // let start = Instant::now();
    // let _df = FileHandler::read_csv_with_polars_convert_to_arrow(source_file_name)?;
    // let duration = start.elapsed();

    // write!(file, "Time elapsed with Rust Polars and convert to json: {} seconds to read {} which size is {} bytes.\n",
    //     duration.as_secs_f64(), source_file_name, file_size)?;

    // Read from BigQuery   
    // let start = Instant::now();
    // let rt = Runtime::new()?;
    // rt.block_on(FileHandler::read_from_bigquery())?;
    // let duration = start.elapsed();

    // write!(file, "Time elapsed to read and write to file from BigQuery: {} seconds to read table and write it to .txt file.\n",
    //     duration.as_secs_f64())?;

    // // Load data to BigQuery
    // let start = Instant::now();
    // FileHandler::load_csv_to_bigquery("data_tests", "chess_games", "files-to-experiment", "chess_games.csv")?;
    // let duration = start.elapsed();

    // write!(file, "Time elapsed to load data to BigQuery: {} seconds to load data to BigQuery.\n",
    //     duration.as_secs_f64())?;

    Ok(())
}