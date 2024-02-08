use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use tokio::time::{Duration, sleep};

#[tokio::main]
async fn main() {
    let echo_uri = Url::parse("wss://www.seismicportal.eu/standing_order/websocket").unwrap();
    let (ws_stream, _) = tokio_tungstenite::connect_async(echo_uri)
        .await
        .expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    let read = read.for_each(|message| async {
        match message {
            Ok(msg) => {
                if msg.is_text() || msg.is_binary() {
                    myprocessing(msg.to_text().unwrap());
                }
            }
            Err(e) => {
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

fn myprocessing(message: &str) {
    let data: serde_json::Value = serde_json::from_str(message).unwrap();
    let info = data["data"]["properties"].as_object().unwrap();
    let action = data["action"].as_str().unwrap();
    println!(
        ">>>> {action:7} event from {auth:7}, unid:{unid}, T0:{time}, Mag:{mag}, Region: {flynn_region}",
        action = action,
        auth = info["auth"].as_str().unwrap(),
        unid = info["unid"].as_str().unwrap(),
        time = info["time"].as_str().unwrap(),
        mag = info["mag"].as_str().unwrap(),
        flynn_region = info["flynn_region"].as_str().unwrap()
    );
}