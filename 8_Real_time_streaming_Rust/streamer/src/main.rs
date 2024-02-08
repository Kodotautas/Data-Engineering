use actix_rt::System;
use awc::Client;

fn main() {
    System::new().block_on(async {
        let client = Client::default();

        let res = client
            .get("https://www.seismicportal.eu/standing_order")    // <- Create request builder
            .insert_header(("User-Agent", "Actix-web"))
            .send()                             // <- Send http request
            .await;

        println!("Response: {:?}", res);        // <- server http response
    });
}