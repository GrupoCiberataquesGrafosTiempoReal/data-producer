use std::env;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Config {
    pub is_interactive: bool,
    pub file_path: PathBuf,
    pub batch_size: usize,
    pub delay_ms: usize,
    pub kafka_brokers: String,
    pub kafka_topic: String
}

pub fn new_config(is_interactive: bool, kafka_brokers: String, kafka_topic: String) -> Config {
    Config {
        is_interactive,
        file_path: PathBuf::new(),
        batch_size: 1,
        delay_ms: 1000,
        kafka_brokers,
        kafka_topic,
    }
}

pub fn environmental_config(config: &mut Config) {
    let file_path = env::var("FILE_PATH");
    config.file_path = PathBuf::from(file_path.expect("FILE_PATH environment variable not set"));

    let batch_size = env::var("BATCH_SIZE").unwrap_or(config.batch_size.to_string());
    config.batch_size = match batch_size.parse::<usize>() {
        Ok(value) => value,
        _ => config.batch_size,
    };

    let delay_ms = env::var("DELAY_MS").unwrap_or(config.delay_ms.to_string());
    config.delay_ms = match delay_ms.parse::<usize>() {
        Ok(value) => value,
        _ => config.delay_ms,
    };
}