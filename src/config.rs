use anyhow::{anyhow, Result};
use std::env;
use std::path::PathBuf;
use crate::constants::{DEFAULT_BATCH_SIZE, DEFAULT_DELAY_MS};

#[derive(Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub file_path: PathBuf,
    pub batch_size: usize,
    pub delay_ms: usize,
}

pub fn new() -> Result<Config> {
    dotenvy::dotenv().ok();

    let kafka_brokers = env::var("KAFKA_BROKERS")
        .map_err(|_| anyhow!("KAFKA_BROKERS environment variable not set"))?;
    
    let kafka_topic = env::var("KAFKA_TOPIC")
        .map_err(|_| anyhow!("KAFKA_TOPIC environment variable not set"))?;

    let file_path = PathBuf::from(
        env::var("FILE_PATH")
            .unwrap_or_else(|_| String::new())
    );

    let batch_size = env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);

    let delay_ms = env::var("DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_DELAY_MS);

    Ok(Config {
        kafka_brokers,
        kafka_topic,
        file_path,
        batch_size,
        delay_ms,
    })
}