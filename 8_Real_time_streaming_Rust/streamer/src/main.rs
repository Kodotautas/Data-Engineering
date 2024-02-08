use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use tokio::time::{Duration, sleep};
struct Pipeline;

#[tokio::main]
async fn main() {
    let echo_uri = Url::parse("wss://www.seismicportal.eu/standing_order/websocket").unwrap();
    let (ws_stream, _) = tokio_tungstenite::connect_async(echo_uri)
        .await
        .expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    let read = read.for_each(|message| async {
        if let Ok(msg) = message {
            if msg.is_text() || msg.is_binary() {
                processing(msg.to_text().unwrap_or_default());
            }
        } else if let Err(e) = message {
            println!("Error in message: {}", e);
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

impl Pipeline{
    fn processing(message: &str) {
        let data: serde_json::Value = serde_json::from_str(message).unwrap();
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

        // concat the message to a file and constantly append to it
        let mut _file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("streamer.txt")
            .unwrap();

        let _ = writeln!(_file, "{}", message);
    }
}