use aws_sdk_sqs::{Client, output::SendMessageOutput};
use std::{thread, time::Duration};

const QUEUE_NAME: &str = "TestQueue.fifo";

#[tokio::main]
async fn main() {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);

    let queue_url = get_queue_url(&client).await;
    let queue_url = match queue_url {
        Some(queue_url) => queue_url,
        None => String::new(),
    };
    for i in 0..10 {
        let message = format!("Message NR {}", i);
        let res = send_message(&client, &queue_url, &message).await;
        match res {
            Some(res) => println!("{:#?}", res),
            _ => println!("Error"),
        };
        thread::sleep(Duration::from_millis(1000));
    };
}

async fn get_queue_url(client: &Client) -> Option<String> {
    let get_queue_url = client.get_queue_url();
    let queue_url = get_queue_url.queue_name(QUEUE_NAME).send().await.ok()?;
    queue_url.queue_url
}

async fn send_message(client: &Client, queue_url: &str, message: &str) -> Option<SendMessageOutput> {
    client.send_message()
        .queue_url(queue_url)
        .message_body(message)
        .message_group_id("1")
        .send()
        .await.ok()
}
