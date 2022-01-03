use aws_sdk_sqs::client::fluent_builders;
use aws_sdk_sqs::model::MessageAttributeValue;
use aws_sdk_sqs::{Client, output::SendMessageOutput, output::GetQueueUrlOutput};
use std::{thread, time::Duration};
use std::error::Error;

const QUEUE_NAME: &str = "TestQueue.fifo";

struct SqsHandler { 
    client: Client,
}

impl SqsHandler {
    fn new(client: Client) -> SqsHandler {
        SqsHandler { client }
    }
}

#[async_trait::async_trait]
pub trait MessageHandler {
    fn get_queue_url(&self) -> fluent_builders::GetQueueUrl;
    fn send_message(&self) -> fluent_builders::SendMessage;
}

#[async_trait::async_trait]
impl MessageHandler for SqsHandler {
    fn get_queue_url(&self) -> fluent_builders::GetQueueUrl {
        self.client.get_queue_url()
    }

    fn send_message(&self) -> fluent_builders::SendMessage {
        self.client.send_message()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);
    let message_handler = SqsHandler::new(client);

    let queue_url = get_queue_url(&message_handler).await?;
    let GetQueueUrlOutput{ queue_url, .. } = queue_url;
    if let Some(queue_url) = queue_url {
        for i in 0..10 {
            let message = format!("Message NR {}", i);
            let res = send_message(&message_handler, &queue_url, &message).await;
            match res {
                Some(res) => println!("{:#?}", res),
                _ => println!("Error"),
            };
            thread::sleep(Duration::from_millis(1000));
        };
    };

    Ok(())
}

async fn get_queue_url(message_handler: &dyn MessageHandler) -> Result<GetQueueUrlOutput, Box<dyn Error + Send + Sync + 'static>> {
    let get_queue_url = message_handler.get_queue_url();
    let queue_url = get_queue_url.queue_name(QUEUE_NAME).send().await?;
    Ok(queue_url)
}

async fn send_message(message_handler: &dyn MessageHandler, queue_url: &str, message: &str) -> Option<SendMessageOutput> {
    message_handler.send_message()
        .queue_url(queue_url)
        .message_body(message)
        .message_group_id("1")
        .send()
        .await.ok()
}
