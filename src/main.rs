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

#[derive(Clone)]
pub enum Response {
    GetQueueUrl{
        url: Option<String>,
    },
    SendMessage{
        response: Option<String>,
    },
    Error,
}

#[async_trait::async_trait]
pub trait MessageHandler {
    async fn get_queue_url(&self, queue_name: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>>;

    async fn send_message(&self, queue_url: &str, message: &str, group_id: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>>;
}

#[async_trait::async_trait]
impl MessageHandler for SqsHandler {
    async fn get_queue_url(&self, queue_name: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>> {
        let get_queue_url = self.client.get_queue_url();
        let GetQueueUrlOutput { queue_url, .. } = get_queue_url.queue_name(queue_name).send().await?;
        Ok(
            Response::GetQueueUrl {
                url: queue_url,
            }
        )
    }

    async fn send_message(&self, queue_url: &str, message: &str, group_id: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>> {
        self.client.send_message()
            .queue_url(queue_url)
            .message_body(message)
            .message_group_id(group_id)
            .send()
            .await?;

        Ok(
            Response::SendMessage {
               response: Some("Message sent successfully!".to_string())
            }
        )
    }
}

async fn get_queue_url(client: &dyn MessageHandler, queue_name: &str) -> String {
    if let Ok(Response::GetQueueUrl{ url, .. }) = client.get_queue_url(queue_name).await {
        if let Some(url) = url {
            url
        } else {
            "".to_string()
        }
    } else {
        println!("Error, queue name '{}' cannot be found!", queue_name);
        "".to_string()
    }
}

async fn send_message(client: &dyn MessageHandler, queue_url: &str, message: &str, group_id: &str) {
    if let Ok(Response::SendMessage{ response }) = client.send_message(queue_url, message, group_id).await {
        match response {
            Some(res) => println!("{}", res),
            _ => println!("Error"),
        }
    }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let shared_config = aws_config::load_from_env().await;
    let client = Client::new(&shared_config);
    let message_handler: &dyn MessageHandler = &SqsHandler::new(client);

    let url = get_queue_url(message_handler, &QUEUE_NAME).await; 
    for i in 0..10 {
        let message = format!("Message NR {}", i);
        send_message(message_handler, &url, &message, &"1").await;
        thread::sleep(Duration::from_millis(1000));
    };

    Ok(())
}


#[cfg(test)]
mod test {
    use super::*;

    struct MockSqsClient {
        queue_url: String,
        response: Response,
    }

    #[async_trait::async_trait]
    impl MessageHandler for MockSqsClient {
        async fn get_queue_url(&self, _queue_name: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>> {
            Ok(
                Response::GetQueueUrl{
                    url: Some(self.queue_url.clone()),
                }
            )
        }

        async fn send_message(&self, _queue_url: &str, _message: &str, _group_id: &str) -> Result<Response, Box<dyn Error + Send + Sync + 'static>> {
            Ok(self.response.clone())
        }
    }

}
