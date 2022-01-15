use aws_sdk_sqs::client::fluent_builders;
use aws_sdk_sqs::{
    Client,
    output::GetQueueUrlOutput,
};
use std::{thread, time::Duration};
use std::error::Error;

const QUEUE_NAME: &str = "TestQueue.fifo";

struct SqsHandler { 
    client: Client,
}

impl SqsHandler {
    fn new(client: Client) -> Self {
        Self { client }
    }
}

struct GetQueueUrlResult {
    name: String,
    url: Option<String>,
}

struct SendMessageResult {
    response: Option<String>,
}

#[async_trait::async_trait]
pub trait MessageHandler {
    async fn get_queue_url(&self, queue_name: &str) -> Result<GetQueueUrlResult, Box<dyn Error + Send + Sync + 'static>>;
    async fn send_message(&self, queue_url: &str, message: &str, group_id: &str) -> Result<SendMessageResult, Box<dyn Error + Send + Sync + 'static>>;
}

#[async_trait::async_trait]
impl MessageHandler for SqsHandler {
    async fn get_queue_url(&self, queue_name: &str) -> Result<GetQueueUrlResult, Box<dyn Error + Send + Sync + 'static>> {
        let get_queue_url = self.client.get_queue_url();
        let GetQueueUrlOutput { queue_url, .. } = get_queue_url.queue_name(queue_name).send().await?;
        Ok(
            GetQueueUrlResult {
                name: queue_name.to_string(),
                url: queue_url,
            }
        )
    }

    async fn send_message(&self, queue_url: &str, message: &str, group_id: &str) -> Result<SendMessageResult, Box<dyn Error + Send + Sync + 'static>> {
        self.client.send_message()
            .queue_url(queue_url)
            .message_body(message)
            .message_group_id(group_id)
            .send()
            .await?;

        Ok(
            SendMessageResult {
               response: Some("Message sent successfully!".to_string())
            }
        )
    }
}

async fn get_queue_url(client: &dyn MessageHandler, queue_name: &str) -> Result<String, Box<dyn Error + Send + Sync + 'static>> {
    let GetQueueUrlResult{ name, url } = client.get_queue_url(queue_name).await?;
    if let Some(url) = url {
        Ok(url)
    } else {
        Ok("".to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);
    let message_handler: MessageHandler = SqsHandler::new(client);

    let queue_url = message_handler.get_queue_url().await?;
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


#[cfg(test)]
mod test {
    use aws_sdk_sqs::operation::GetQueueUrl;

    use super::*;

    struct MockSqsClient {
        queue_name: String,
    }

    #[async_trait::async_trait]
    impl MessageHandler for MockSqsClient {
        fn get_queue_url(&self) -> fluent_builders::GetQueueUrl {
        }

        fn send_message(&self) -> fluent_builders::SendMessage {
        }
    }
}
