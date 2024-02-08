use tungstenite::{client, Message};
use url::Url;
use serde_json::Value;
use std::time::Duration;
use std::thread;

fn main() {
    let url = Url::parse("wss://www.seismicportal.eu/standing_order/websocket").unwrap();
    let (mut socket, _response) = match client::connect(url) {
        Ok((socket, response)) => (socket, response),
        Err(e) => {
            println!("Error: {}", e);
            return;
        }
    };

    println!("Connected to the server");

    loop {
        let msg = socket.read().expect("Error reading message");
        match msg {
            Message::Text(text) => myprocessing(text),
            Message::Close(_) => {
                println!("Server disconnected");
                break;
            }
            _ => {}
        }
        thread::sleep(Duration::from_secs(15));
    }
}

fn myprocessing(message: String) {
    let v: Value = serde_json::from_str(&message).unwrap();
    match v["data"]["properties"].as_object() {
        Some(properties) => {
            let action = v["action"].as_str().unwrap_or("");
            let auth = properties.get("auth").and_then(Value::as_str).unwrap_or("");
            let unid = properties.get("unid").and_then(Value::as_str).unwrap_or("");
            let time = properties.get("time").and_then(Value::as_str).unwrap_or("");
            let mag = properties.get("mag").and_then(Value::as_str).unwrap_or("");
            let flynn_region = properties.get("flynn_region").and_then(Value::as_str).unwrap_or("");
            println!(">>>> {action:7} event from {auth:7}, unid:{unid}, T0:{time}, Mag:{mag}, Region: {flynn_region}",
                     action = action, auth = auth, unid = unid, time = time, mag = mag, flynn_region = flynn_region);
        }
        None => println!("Unable to parse json message"),
    }
}