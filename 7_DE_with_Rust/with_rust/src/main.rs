use std::fs;
use std::env;
use std::io;
use std::fs::File;
use std::process::Command;
use std::error::Error;
use tokio::runtime::Runtime;
use polars::prelude::*;
use gcp_bigquery_client::model::query_request::QueryRequest;
use csv::ReaderBuilder;
use csv::StringRecord;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
struct FileHandler;
struct Streamer;


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
    

    fn csv_to_arrow(csv_file: &str, schema_json: &str, arrow_file: &str) -> io::Result<()> {
        let output = Command::new("csv2arrow")
            .arg("--schema-file")
            .arg(schema_json)
            .arg(csv_file)
            .arg(arrow_file)
            .output()?;
    
        if !output.status.success() {
            eprintln!("Error: {}", String::from_utf8_lossy(&output.stderr));
        }
    
        Ok(())
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

impl Streamer {
    fn start_server(stop_flag: Arc<Mutex<bool>>) {
        let listener = TcpListener::bind("127.0.0.1:12345").unwrap();
        listener.set_nonblocking(true).expect("Cannot set non-blocking");
        println!("Server started, waiting for connections...");
    
        loop {
            match listener.accept() {
                Ok((mut stream, addr)) => {
                    println!("Connection from {}", addr);
    
                    let mut buffer = [0; 1024];
                    while let Ok(n) = stream.read(&mut buffer) {
                        if n == 0 {
                            break;
                        }
    
                        println!("Received data: {}", String::from_utf8_lossy(&buffer[..n]));
                        stream.write_all(b"Received").unwrap();
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if *stop_flag.lock().unwrap() {
                        break;
                    }
                    // sleep for a while
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(e) => panic!("encountered IO error: {}", e),
            }
        }
    
        println!("Server stopped");
    }

    fn start_client() {
        let mut latencies = Vec::new();
        thread::sleep(Duration::from_secs(1)); // Ensure server is up

        let mut stream = TcpStream::connect("127.0.0.1:12345").unwrap();
        let start_time = Instant::now();

        while start_time.elapsed() < Duration::from_secs(30) {
            let send_time = Instant::now();
            stream.write_all(b"Hello, Server!").unwrap();
            let mut buffer = [0; 1024];
            stream.read(&mut buffer).unwrap();
            let receive_time = Instant::now();

            let latency = receive_time.duration_since(send_time);
            latencies.push(latency);

            println!("Latency: {:.10} seconds", latency.as_secs_f64());

            thread::sleep(Duration::from_secs(1)); // Delay for 1 second
        }

        let average_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let average_latency_ms = average_latency.as_secs_f64() * 1000.0;
        println!("Average Latency: {:.10} milliseconds", average_latency_ms);

        // add latency to file
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open("src/times.txt").unwrap();

        write!(file, "Average Latency: {:.10} milliseconds\n", average_latency_ms).unwrap();
    }
    
    
    fn main_stream_processor() {
        let stop_flag = Arc::new(Mutex::new(false));
    
        let server_stop_flag = Arc::clone(&stop_flag);
        let server_thread = thread::spawn(move || {
            Streamer::start_server(server_stop_flag);
        });
    
        let client_thread = thread::spawn(|| {
            Streamer::start_client();
        });
    
        client_thread.join().unwrap();
        *stop_flag.lock().unwrap() = true; // Signal the server to stop
    
        server_thread.join().unwrap();

    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_file_name = "/home/vytautas/Desktop/chess_games.csv";
    let schema_json = "/home/vytautas/Desktop/chess_schema.json";

    // Read csv file with Polars
    let start = Instant::now();
    let _df = FileHandler::read_csv_with_polars(source_file_name)?;
    let duration = start.elapsed();

    let metadata = fs::metadata(source_file_name)?;
    let file_size = metadata.len();

    // set apend file
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open("src/times.txt")?;

    let mut file = File::create("src/times.txt")?;
    write!(file, "Time elapsed with Rust Polars: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    // Read csv file with Polars and drop null values
    let start = Instant::now();
    let _df = FileHandler::read_csv_with_polars_drop_nulls(source_file_name)?;
    let duration = start.elapsed();

    write!(file, "Time elapsed with Rust Polars and drop null values: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;


    // Read csv file with Polars and filter event column
    let start = Instant::now();
    let _df = FileHandler::read_csv_and_filter_event_column(source_file_name)?;
    let duration = start.elapsed();

    write!(file, "Time elapsed with Rust Polars and filter event column: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    // csv to parquet
    let start = Instant::now();
    let _df = FileHandler::csv_to_arrow(source_file_name, schema_json, "chess_games.arrow")?;
    let duration = start.elapsed();

    write!(file, "Time elapsed with Rust convert to arrow: {} seconds to read {} which size is {} bytes.\n",
        duration.as_secs_f64(), source_file_name, file_size)?;

    // Read from BigQuery   
    let start = Instant::now();
    let rt = Runtime::new()?;
    rt.block_on(FileHandler::read_from_bigquery())?;
    let duration = start.elapsed();

    write!(file, "Time elapsed to read and write to file from BigQuery: {} seconds to read table and write it to .txt file.\n",
        duration.as_secs_f64())?;

    // Load data to BigQuery
    let start = Instant::now();
    FileHandler::load_csv_to_bigquery("data_tests", "chess_games", "files-to-experiment", "chess_games.csv")?;
    let duration = start.elapsed();

    write!(file, "Time elapsed to load data to BigQuery: {} seconds to load data to BigQuery.\n",
        duration.as_secs_f64())?;

    // lauch stream processor
    Streamer::main_stream_processor();

    Ok(())
    
}