mod config;
mod constants;
mod interface;
mod utils;

use clap::Parser;
use anyhow::Result;
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

    let mut config = config::new()?;

    if !args.no_interactive {
        interface::cli::configure(&mut config);
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