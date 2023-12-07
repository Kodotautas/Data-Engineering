use std::fs::File;
use std::io::Write;
use std::time::Instant;
use std::os::unix::fs::MetadataExt;
use polars::prelude::*;

struct FileHandler;

impl FileHandler {
    fn read_csv_with_polars(file_name: &str) -> Result<DataFrame> {
        let df = CsvReader::from_path(file_name)?.finish()?;
        Ok(df)
    }
}

fn main() -> Result<()> {
    let source_file_name = "/home/vytautas/Desktop/alban_news.csv";

    let start = Instant::now();
    let _ = FileHandler::read_csv_with_polars(source_file_name)?;
    let duration = start.elapsed();

    let metadata = fs::metadata(source_file_name)?;
    let file_size = metadata.len();

    let mut file = File::create("7_DE_with_Rust/1_read_upload_csv/with_rust/src/times.txt")?;
    write!(file, "Time elapsed with Polars: {} seconds to read {} which size is {} bytes.\n", 
        duration.as_secs_f64(), source_file_name, file_size)?;

    Ok(())
}