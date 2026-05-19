mod config;
mod interface;
mod utils;

use anyhow::{anyhow, Result};
use std::env;
use utils::file_utils::list_supported_files;
use utils::processors::{process_csv, process_parquet};
use utils::producer::create_kafka_producer;

fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    loop {
        let folder = env::var("DATA_FOLDER")
            .map_err(|_| anyhow!("DATA_FOLDER environment variable not set"))?;

        let files = list_supported_files(&folder)?;

        if files.is_empty() {
            return Err(anyhow!("No CSV or Parquet files found"));
        }

        let kafka_brokers = env::var("KAFKA_BROKERS")
            .map_err(|_| anyhow!("KAFKA_BROKERS environment variable not set"))?;
        
        let kafka_topic = env::var("KAFKA_TOPIC")
            .map_err(|_| anyhow!("KAFKA_TOPIC environment variable not set"))?;

        let config = interface::cli::configure(&files, kafka_brokers, kafka_topic)?;

        let path = &files[config.file_index];

        let extension = path
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();

        let producer = create_kafka_producer(&config)?;

        let should_restart = if extension == "parquet" {
            process_parquet(path, &config, producer)?
        } else {
            process_csv(path, &config, producer)?
        };

        if !should_restart {
            break;
        }
    }

    Ok(())
}