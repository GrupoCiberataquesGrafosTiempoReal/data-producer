mod config;
mod interface;
mod utils;

use anyhow::{anyhow, Result};
use std::env;
use utils::file_utils::list_supported_files;
use utils::readers::{process_csv, process_parquet};

fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    loop {
        let folder = env::var("DATA_FOLDER")
            .map_err(|_| anyhow!("DATA_FOLDER environment variable not set"))?;

        let files = list_supported_files(&folder)?;

        if files.is_empty() {
            return Err(anyhow!("No CSV or Parquet files found"));
        }

        let config = interface::cli::configure(&files)?;

        let path = &files[config.file_index];

        let extension = path
            .extension()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();

        let should_restart = if extension == "parquet" {
            process_parquet(path, &config)?
        } else {
            process_csv(path, &config)?
        };

        if !should_restart {
            break;
        }
    }

    Ok(())
}