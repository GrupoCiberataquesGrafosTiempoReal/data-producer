use crate::config::Config;

use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, Producer, BaseRecord};
use serde_json::Value;
use std::time::Duration;

pub fn create_kafka_producer(config: &Config) -> Result<BaseProducer> {
    let producer: BaseProducer = ClientConfig::new()
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

pub fn publish_batch(
    batch: &[Value],
    producer: &BaseProducer,
    topic: &str,
) -> Result<()> {
    for json in batch {
        let payload = serde_json::to_string(json)?;

        producer
            .send(
                BaseRecord::to(topic)
                    .key("row")
                    .payload(&payload)
            )
            .map_err(|(err, _)| anyhow!("Kafka send error: {}", err))?;
    }

    producer.poll(Duration::from_millis(0));

    Ok(())
}