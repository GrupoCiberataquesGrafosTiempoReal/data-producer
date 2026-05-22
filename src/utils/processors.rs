use crate::config::Config;
use crate::interface::summary::{create_summary, print_summary};
use crate::utils::producer::publish_batch;
use anyhow::Result;
use csv::StringRecord;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use std::fs::File;
use std::thread;
use std::time::{Duration, Instant};
use rdkafka::producer::{FutureProducer};
use serde_json::{Map, Value};

pub async fn process_csv(config: &Config, producer: FutureProducer) -> Result<()> {
    let mut reader = csv::Reader::from_path(config.file_path.clone())?;

    let headers = reader.headers()?.clone();

    let mut batch: Vec<Value> = vec![];
    let mut batch_number = 0usize;

    for record in reader.records() {
        let json = csv_record_to_json(&headers, &(record?));

        batch.push(json);

        if batch.len() >= config.batch_size {
            batch_number += 1;

            if batch_number > 1 {
                thread::sleep(Duration::from_millis(config.delay_ms as u64));
            }

            handle_batch(batch_number, &batch, &producer, &config.kafka_topic).await?;

            batch.clear();
        }
    }

    if !batch.is_empty() {
        thread::sleep(Duration::from_millis(config.delay_ms as u64));
        batch_number += 1;
        handle_batch(batch_number, &batch, &producer, &config.kafka_topic).await?;
    }

    Ok(())
}

fn csv_record_to_json(headers: &StringRecord, record: &StringRecord) -> Value {
    let mut map = Map::new();

    for (header, value) in headers.iter().zip(record.iter()) {
        map.insert(header.to_string(), parse_csv_value(value));
    }

    Value::Object(map)
}

fn parse_csv_value(value: &str) -> Value {
    let v = value.trim();

    if v.is_empty() {
        return Value::Null;
    }

    match v {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }

    if let Ok(i) = v.parse::<i64>() {
        return Value::Number(i.into());
    }

    if let Ok(f) = v.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }

    Value::String(v.to_string())
}

pub async fn process_parquet(config: &Config, producer: FutureProducer) -> Result<()> {
    let file = File::open(config.file_path.clone())?;
    let reader = SerializedFileReader::new(file)?;
    let iter = reader.get_row_iter(None)?;

    let mut batch: Vec<Value> = vec![];
    let mut batch_number = 0usize;

    for row in iter {
        let json = parquet_row_to_json(&(row?));

        batch.push(json);

        if batch.len() >= config.batch_size {
            batch_number += 1;

            if batch_number > 1 {
                thread::sleep(Duration::from_millis(config.delay_ms as u64));
            }

            handle_batch(batch_number, &batch, &producer, &config.kafka_topic).await?;

            batch.clear();
        }
    }

    if !batch.is_empty() {
        thread::sleep(Duration::from_millis(config.delay_ms as u64));
        batch_number += 1;
        handle_batch(batch_number, &batch, &producer, &config.kafka_topic).await?;
    }

    Ok(())
}

fn parquet_row_to_json(row: &Row) -> Value {
    let mut map = Map::new();

    for (name, field) in row.get_column_iter() {
        map.insert(
            name.to_string(),
            match field {
                Field::Null => Value::Null,
                Field::Str(v) => Value::String(v.clone()),
                Field::Double(v) => Value::from(*v),
                Field::Int(v) => Value::from(*v),
                Field::Long(v) => Value::from(*v),
                Field::Bool(v) => Value::Bool(*v),
                Field::TimestampMillis(ms) => {
                    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(*ms);
                    match dt {
                        Some(dt) => Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
                        None => Value::Null,
                    }
                },
                _ => Value::String(format!("{:?}", field)),
            }
        );
    }

    Value::Object(map)
}

async fn handle_batch(
    batch_number: usize,
    batch: &[Value],
    producer: &FutureProducer,
    topic: &str,
) -> Result<()> {
    let start = Instant::now();

    publish_batch(batch, producer, &topic).await?;

    let elapsed = start.elapsed().as_millis();

    let summary = create_summary(
        batch_number,
        batch.len(),
        elapsed,
    );
    print_summary(summary);

    Ok(())
}