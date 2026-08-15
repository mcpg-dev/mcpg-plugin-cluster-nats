use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use mcpg_cluster_api::{ClusterError, Message, PubSub, Subscription};

/// Core NATS pub/sub. Stateless, fire-and-forget, supports queue
/// groups for load-balanced subscribers.
#[derive(Debug, Clone)]
pub struct NatsTopicBus {
    client: Client,
}

impl NatsTopicBus {
    /// Construct a `NatsTopicBus` over an already-connected
    /// `async_nats::Client`. Used by `mcpg-plugin-cluster-nats` to
    /// share its single connection with the pub/sub primitive
    /// accessor instead of opening a fresh connection.
    pub fn with_client(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl PubSub for NatsTopicBus {
    async fn publish(&self, topic: &str, payload: Bytes) -> Result<(), ClusterError> {
        self.client
            .publish(topic.to_owned(), payload)
            .await
            .map_err(|e| ClusterError::BackendUnavailable {
                reason: format!("nats publish `{topic}`: {e}"),
            })
    }

    async fn subscribe(
        &self,
        pattern: &str,
        queue_group: Option<&str>,
    ) -> Result<Subscription, ClusterError> {
        let sub = if let Some(group) = queue_group {
            self.client
                .queue_subscribe(pattern.to_owned(), group.to_owned())
                .await
        } else {
            self.client.subscribe(pattern.to_owned()).await
        }
        .map_err(|e| ClusterError::BackendUnavailable {
            reason: format!("nats subscribe `{pattern}`: {e}"),
        })?;
        let stream = sub.map(|msg| {
            Ok(Message {
                topic: msg.subject.to_string(),
                payload: msg.payload,
            })
        });
        Ok(Box::pin(stream))
    }
}
