use std::io::{BufReader, BufWriter};
use std::fs::{File, read_dir};
use zip::write::FileOptions;
use zip::CompressionMethod::Deflated;
use std::time::Instant;

fn zip_folder(folder_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let zip_file_path = format!("{}.zip", folder_path);
    let file = BufWriter::new(File::create(&zip_file_path)?);
    let mut zip = zip::ZipWriter::new(file);

    let options = FileOptions::default()
        .compression_method(Deflated)
        .unix_permissions(0o755);

    for entry in read_dir(folder_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            zip.start_file(file_name, options)?;
            let mut file = BufReader::new(File::open(path)?);
            std::io::copy(&mut file, &mut zip)?;
        }
    }

    zip.finish()?;

    Ok(zip_file_path)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_zip = Instant::now();

    let folder_path = "/home/vytautas/Desktop/us_elections";
    let _zip_file_path = zip_folder(&folder_path)?;

    let duration_zip = start_zip.elapsed();
    println!("Folder zipped in {} minutes", duration_zip.as_secs_f64() / 60.0);

    Ok(())
}