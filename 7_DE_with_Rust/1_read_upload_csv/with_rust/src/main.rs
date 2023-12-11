use std::fs;
use std::fs::File;
use std::io::Write;
use std::time::Instant;
use polars::prelude::*;
use google_bigquery2::Bigquery;
use yup_oauth2 as oauth2;
use google_bigquery2::api::TableDataInsertAllRequest;
use serde_json::json;

struct FileHandler;

impl FileHandler {
    fn read_csv_with_polars(file_name: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
        let df = CsvReader::from_path(file_name)?.finish()?;
        Ok(df)
    }

    async fn upload_to_bigquery(df: DataFrame, project_id: &str, dataset_id: &str, table_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let secret = oauth2::read_service_account_key("/home/vytautas/.config/gcloud/application_default_credentials.json").await?;
        let auth = oauth2::ServiceAccountAuthenticator::builder(secret)
            .build()
            .await?;
    
        let https = hyper_rustls::HttpsConnector::with_native_roots();
        let client = hyper::client::builder().build::<_, hyper::Body>(https);
        let mut bigquery = Bigquery::new(client, auth);

        let mut rows = Vec::new();
        for row in df.iter_rows() {
            let mut json_row = json!({});
            for (column, value) in row {
                json_row[column] = json!(value);
            }
            rows.push(json_row);
        }

        let mut request = TableDataInsertAllRequest::default();
        request.rows = Some(rows);
        let response = bigquery.tabledata().insert_all(request, project_id, dataset_id, table_id).doit().await?;
        println!("{:?}", response);
        
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_file_name = "/home/vytautas/Desktop/chess_games.csv";
    let project_id = "data-engineering-with-rust";
    let dataset_id = "data_tests";
    let table_id = "chess_games";

    let start = Instant::now();
    let df = FileHandler::read_csv_with_polars(source_file_name)?;
    let duration = start.elapsed();

    let metadata = fs::metadata(source_file_name)?;
    let file_size = metadata.len();

    let mut file = File::create("src/times.txt")?;
    write!(file, "Time elapsed with Rust Polars: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    let start = Instant::now();
    let _ = FileHandler::upload_to_bigquery(df, project_id, dataset_id, table_id)?;
    let duration = start.elapsed();

    write!(file, "Time elapsed with Rust Polars read / upload: {} seconds to upload {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;
}