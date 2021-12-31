use aws_sdk_sqs::{Client, Error, output::GetQueueUrlOutput};
use std::process::exit;

#[tokio::main]
async fn main() {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);
    let get_queue_url = client.get_queue_url();
    let queue_url = get_queue_url.queue_name("TestQueue.fifo").send().await;

    match queue_url.unwrap() {
        GetQueueUrlOutput{ queue_url, .. } => println!("url: {}", queue_url.unwrap()),
        SdkError => println!("Something goes wrong!"),
        _ => println!("..."),
    }
}
