use crate::config::Config;

use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer, FutureRecord};
use serde_json::Value;
use std::time::Duration;
use futures::future::try_join_all;

pub fn create_kafka_producer(config: &Config) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .create()?;

    producer
        .client()
        .fetch_metadata(None, Duration::from_secs(5))
        .map_err(|err| anyhow!("Could not connect to Kafka brokers: {}", err))?;

    Ok(producer)
}

pub async fn publish_batch(
    batch: &[Value],
    producer: &FutureProducer,
    topic: &str,
) -> Result<()> {
    let futures = batch.iter().map(|json| {
        let producer = producer.clone();
        let topic = topic.to_string();
        let json = json.clone();

        async move {
            let payload = serde_json::to_string(&json)?;

            producer.send(
                FutureRecord::to(&topic)
                    .key("row")
                    .payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(err, _)| anyhow!("Kafka send error: {}", err))?;
        
            Ok::<(), anyhow::Error>(())
        }
    });

    try_join_all(futures).await?;

    Ok(())
}