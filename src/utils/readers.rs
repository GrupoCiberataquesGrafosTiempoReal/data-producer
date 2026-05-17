use crate::config::Config;
use crate::interface::interruption::interruption_requested;
use crate::interface::summary::{create_summary, print_summaries, BatchSummary};
use anyhow::Result;
use csv::StringRecord;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

pub fn process_csv(path: &Path, config: &Config) -> Result<bool> {
    let mut reader = csv::Reader::from_path(path)?;

    let mut batch: Vec<StringRecord> = vec![];
    let mut batch_number = 0usize;
    let mut summaries: VecDeque<BatchSummary> = VecDeque::new();

    for record in reader.records() {
        batch.push(record?);

        if batch.len() >= config.batch_size {
            batch_number += 1;

            if handle_batch(batch_number, &batch, &mut summaries)? {
                return Ok(true);
            }

            batch.clear();

            thread::sleep(Duration::from_millis(config.delay_ms));
        }
    }

    Ok(false)
}

pub fn process_parquet(path: &Path, config: &Config) -> Result<bool> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;

    let mut batch: Vec<Row> = vec![];
    let mut batch_number = 0usize;
    let mut summaries: VecDeque<BatchSummary> = VecDeque::new();

    for row in iter {
        batch.push(row?);

        if batch.len() >= config.batch_size {
            batch_number += 1;

            if handle_batch(batch_number, &batch, &mut summaries)? {
                return Ok(true);
            }

            batch.clear();

            thread::sleep(Duration::from_millis(config.delay_ms));
        }
    }

    Ok(false)
}

fn handle_batch<T>(
    batch_number: usize,
    batch: &[T],
    summaries: &mut VecDeque<BatchSummary>,
) -> Result<bool> {
    let start = Instant::now();

    let elapsed = start.elapsed().as_millis();

    summaries.push_back(create_summary(
        batch_number,
        batch.len(),
        elapsed,
    ));

    while summaries.len() > 5 {
        summaries.pop_front();
    }

    print_summaries(summaries);
    
    println!("Press S to stop");

    interruption_requested()
}