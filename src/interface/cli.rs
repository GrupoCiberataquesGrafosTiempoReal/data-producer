use crate::config::Config;
use anyhow::Result;
use std::io::{self, Write};
use std::path::PathBuf;

pub fn configure(files: &[PathBuf], kafka_brokers: String, kafka_topic: String) -> Result<Config> {
    println!("\nAvailable files:\n");

    for (i, file) in files.iter().enumerate() {
        println!("{}. {}", i + 1, file.display());
    }

    let file_index = ask_number("\nSelect file number", 1, 1, files.len())? - 1;

    let batch_size = ask_number("Batch size", 1, 1, usize::MAX)?;

    let delay_ms = ask_number("Delay between batches (ms)", 1000, 0, usize::MAX)? as u64;

    println!("\nConfiguration:");
    println!("File: {}", files[file_index].display());
    println!("Batch size: {}", batch_size);
    println!("Delay: {} ms", delay_ms);

    let confirm = ask_yes_no("Confirm and start? (y/n)", true)?;

    if !confirm {
        return configure(files, kafka_brokers, kafka_topic);
    }

    println!();

    Ok(Config {
        file_index,
        batch_size,
        delay_ms,
        kafka_brokers,
        kafka_topic
    })
}

pub fn ask_number(
    label: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize> {
    loop {
        print!("{} [default: {}]: ", label, default);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim();

        if input.is_empty() {
            return Ok(default);
        }

        match input.parse::<usize>() {
            Ok(value) if value >= min && value <= max => {
                return Ok(value);
            }
            _ => println!("Invalid value"),
        }
    }
}

pub fn ask_yes_no(label: &str, default: bool) -> Result<bool> {
    let default_str = if default { "y" } else { "n" };

    loop {
        print!("{} [default: {}]: ", label, default_str);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim().to_lowercase();

        if input.is_empty() {
            return Ok(default);
        }

        match input.as_str() {
            "y" | "Y" | "yes" | "Yes" | "YES" => return Ok(true),
            "n" | "N" | "no" | "No" | "NO" => return Ok(false),
            _ => println!("Please answer y or n"),
        }
    }
}