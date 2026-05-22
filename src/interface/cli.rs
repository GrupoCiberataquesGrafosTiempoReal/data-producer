use crate::config::Config;
use crate::utils::file_utils::list_supported_files;
use std::io::{self, Write};

pub fn configure(config: &mut Config) {
    let files_dir = ask_string("\nFiles directory", "./data");

    let files = list_supported_files(files_dir);

    if files.is_empty() {
        println!("No CSV or Parquet files found");
        return configure(config);
    }

    println!("\nAvailable files:\n");

    for (i, file) in files.iter().enumerate() {
        println!("{}. {}", i + 1, file.display());
    }

    let file_index = ask_number("\nSelect file number", 1, 1, files.len()) - 1;

    let batch_size = ask_number("Batch size", config.batch_size, 1, usize::MAX);

    let delay_ms = ask_number("Delay between batches (ms)", config.delay_ms, 0, usize::MAX);

    println!("\nConfiguration:");
    println!("File: {}", files[file_index].display());
    println!("Batch size: {}", batch_size);
    println!("Delay: {} ms", delay_ms);

    let confirm = ask_yes_no("\nConfirm and start? (y/n)", true);

    if !confirm {
        return configure(config);
    }

    println!();

    config.file_path = files[file_index].clone();
    config.batch_size = batch_size;
    config.delay_ms = delay_ms;
}

fn ask_string(label: &str, default: &str) -> String {
    print!("{} [default: {}]: ", label, default);
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    let input = input.trim();

    if input.is_empty() {
        return default.to_string();
    }

    input.to_string()
}

fn ask_number(
    label: &str,
    default: usize,
    min: usize,
    max: usize,
) -> usize {
    loop {
        print!("{} [default: {}]: ", label, default);
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let input = input.trim();

        if input.is_empty() {
            return default;
        }

        match input.parse::<usize>() {
            Ok(value) if value >= min && value <= max => {
                return value;
            }
            _ => println!("Invalid value"),
        }
    }
}

fn ask_yes_no(label: &str, default: bool) -> bool {
    let default_str = if default { "y" } else { "n" };

    loop {
        print!("{} [default: {}]: ", label, default_str);
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let input = input.trim().to_lowercase();

        if input.is_empty() {
            return default;
        }

        match input.as_str() {
            "y" | "Y" | "yes" | "Yes" | "YES" => return true,
            "n" | "N" | "no" | "No" | "NO" => return false,
            _ => println!("Please answer y or n"),
        }
    }
}