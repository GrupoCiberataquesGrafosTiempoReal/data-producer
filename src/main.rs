mod config;
mod interface;
mod utils;

use clap::Parser;
use anyhow::{anyhow, Result};
use std::env;
use utils::processors::{process_csv, process_parquet};
use utils::producer::create_kafka_producer;

#[derive(Parser)]
struct Args {
    /// Run in non-interactive mode
    #[arg(long)]
    no_interactive: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    dotenvy::dotenv().ok();

    let kafka_brokers = env::var("KAFKA_BROKERS")
        .map_err(|_| anyhow!("KAFKA_BROKERS environment variable not set"))?;
    
    let kafka_topic = env::var("KAFKA_TOPIC")
        .map_err(|_| anyhow!("KAFKA_TOPIC environment variable not set"))?;

    let mut config = config::new_config(!args.no_interactive, kafka_brokers, kafka_topic);

    if config.is_interactive {
        interface::cli::configure(&mut config);
    } else {
        config::environmental_config(&mut config);
    }

    let extension = config.file_path
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();

    let producer = create_kafka_producer(&config)?;

    if extension == "parquet" {
        process_parquet(&config, producer).await?;
    } else {
        process_csv(&config, producer).await?;
    }

    Ok(())
}