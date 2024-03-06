use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use tokio::time::sleep;
use std::sync::Arc;
use tokio::sync::Semaphore;
use std::time::{Duration, Instant};
use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::{PubsubMessage};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Server, Body, Response, Request};
use std::convert::Infallible;
use std::net::SocketAddr;

struct Pipeline {
    semaphore: Arc<Semaphore>,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("PORT").unwrap_or_else(|_| String::from("8080"));
    let addr = format!("0.0.0.0:{}", port).parse::<SocketAddr>()?;

    let make_svc = make_service_fn(|_conn| async {
        Ok::<_, Infallible>(service_fn(handle_request))
    });

    let server = Server::bind(&addr).serve(make_svc);

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}

// Handle incoming requests
async fn handle_request(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let echo_uri = Url::parse("wss://www.seismicportal.eu/standing_order/websocket").unwrap();
    let (ws_stream, _) = tokio_tungstenite::connect_async(echo_uri)
        .await
        .expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    let pipeline = Pipeline {
        semaphore: Arc::new(Semaphore::new(10)),
    };

    // Read task for processing messages
    let read = async move {
        read.for_each_concurrent(None, |message| {
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
        }).await;
    };
    
    // Write task for server ping
    let write = async move {
        loop {
            write.send(Message::Ping(vec![])).await.unwrap();
            sleep(Duration::from_secs(15)).await;
        }
    };
    
    let read_handle = tokio::spawn(read);
    let write_handle = tokio::spawn(write);

    let _ = tokio::try_join!(read_handle, write_handle);

    Ok(Response::new(Body::from("OK")))
}

impl Clone for Pipeline {
    fn clone(&self) -> Self {
        Pipeline {
            semaphore: Arc::clone(&self.semaphore),
        }
    }
}

impl Pipeline{
    // Process the incoming message
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

            // Event processing
            let start = Instant::now();
            let load_task = tokio::spawn(async move {
                Self::publish_to_pubsub(&event_str).await.unwrap();
            });

            // Wait for the load operation to complete
            let _ = load_task.await;
            println!("Time elapsed to process event: {:?} seconds", start.elapsed().as_secs_f64());
        });
    }

    // Publish the event to Google Cloud Pub/Sub
    async fn publish_to_pubsub(json: &str) -> Result<(), Box<dyn std::error::Error>> {
        let config = ClientConfig::default().with_auth().await.unwrap();
        let client = Client::new(config).await.unwrap();
        let topic = client.topic("earthquakes-raw");
    
        let message = PubsubMessage {
            data: json.to_string().into_bytes(),
            ..Default::default()
        };
    
        let publisher = topic.new_publisher(None);
        let _ = publisher.publish(message).await;
        println!("Published message: {}", json);
    
        Ok(())
    }
}