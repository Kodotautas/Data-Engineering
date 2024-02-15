use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use tokio::time::sleep;
use std::sync::Arc;
use tokio::sync::Semaphore;
use std::process::Command;
use std::fs::File;
use std::io::Write;
use std::env::temp_dir;
use std::time::{Duration, Instant};

struct Pipeline {
    semaphore: Arc<Semaphore>,
}

#[tokio::main]
async fn main() {
    let echo_uri = Url::parse("wss://www.seismicportal.eu/standing_order/websocket").unwrap();
    let (ws_stream, _) = tokio_tungstenite::connect_async(echo_uri)
        .await
        .expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    let pipeline = Pipeline {
        semaphore: Arc::new(Semaphore::new(10)),  // adjust as needed
    };

    let read = read.for_each_concurrent(None, |message| {
        let pipeline = pipeline.clone();
        async move {
            if let Ok(msg) = message {
                if msg.is_text() || msg.is_binary() {
                    pipeline.processing(msg.to_text().unwrap_or_default()).await;
                }
            } else if let Err(e) = message {
                println!("Error in message: {}", e);
            }
        }
    });

    let write = async {
        loop {
            write.send(Message::Ping(vec![])).await.unwrap();
            sleep(Duration::from_secs(15)).await;
        }
    };

    let _ = futures::join!(read, write);
}

impl Clone for Pipeline {
    fn clone(&self) -> Self {
        Pipeline {
            semaphore: Arc::clone(&self.semaphore),
        }
    }
}


impl Pipeline{
    async fn processing(&self, message: &str) {
        let message = message.to_owned();
        let semaphore = Arc::clone(&self.semaphore);
        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.expect("Failed to acquire semaphore");
            let data: serde_json::Value = serde_json::from_str(&message).unwrap();
            let info = match data["data"]["properties"].as_object() {
                Some(info) => info,
                None => {
                    println!("Error: 'properties' field not found");
                    return;
                }
            };
            
            let action = match data["action"].as_str() {
                Some(action) => action,
                None => {
                    println!("Error: 'action' field not found");
                    return;
                }
            };

            println!(
                ">>>> {action:7} event from {auth:7}, unid:{unid}, T0:{time}, Mag:{mag}, Region: {flynn_region}",
                action = action,
                auth = info["auth"].as_str().unwrap_or("unknown"),
                unid = info["unid"].as_str().unwrap_or("unknown"),
                time = info["time"].as_str().unwrap_or("unknown"),
                mag = info["mag"].as_f64().unwrap_or(0.0),
                flynn_region = info["flynn_region"].as_str().unwrap_or("unknown")
            );

            // Convert the event to a string
            let event_str = serde_json::to_string(&data).unwrap();

            // Convert the JSON to a string
            let dataset = "earthquakes";
            let table = "earthquakes_raw";
            let bucket = "rust-raw";
            let file = "temp.json";

            // Event processing
            let start = Instant::now();
            let load_task = tokio::spawn(async move {
                Self::load_json_to_bigquery(dataset, table, bucket, file, &event_str).await.unwrap();
            });

            // Wait for the load operation to complete
            let _ = load_task.await;
            println!("Time elapsed to process event: {:?} seconds", start.elapsed().as_secs_f64());
        });
    }

    async fn load_json_to_bigquery(dataset: &str, table: &str, bucket: &str, file: &str, json: &str) -> std::io::Result<()> {
        // Write the JSON string to a temporary file
        let mut path = temp_dir();
        path.push(file);
        let mut temp_file = File::create(&path)?;
        write!(temp_file, "{}", json)?;
    
        // Upload the file to Google Cloud Storage
        let output = Command::new("gsutil")
            .arg("cp")
            .arg(path.to_str().unwrap())
            .arg(format!("gs://{}/{}", bucket, file))
            .output()?;
    
        if !output.status.success() {
            eprintln!("Error: {}", String::from_utf8_lossy(&output.stderr));
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to upload file to GCS"));
        }
    
        // Load the file from GCS to BigQuery
        let output = Command::new("bq")
            .arg("load")
            .arg("--autodetect")
            .arg("--source_format=NEWLINE_DELIMITED_JSON")
            .arg(format!("{}.{}", dataset, table))
            .arg(format!("gs://{}/{}", bucket, file))
            .output()?;
    
        if !output.status.success() {
            eprintln!("Error: {}", String::from_utf8_lossy(&output.stderr));
        }
    
        Ok(())
    }
}