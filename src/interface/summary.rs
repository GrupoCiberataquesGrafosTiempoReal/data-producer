use chrono::Local;
use std::collections::VecDeque;
use std::io::{self, Write};

#[derive(Clone)]
pub struct BatchSummary {
    pub batch_number: usize,
    pub rows_read: usize,
    pub elapsed_ms: u128,
    pub timestamp: String,
}

pub fn create_summary(
    batch_number: usize,
    rows_read: usize,
    elapsed_ms: u128,
) -> BatchSummary {
    BatchSummary {
        batch_number,
        rows_read,
        elapsed_ms,
        timestamp: Local::now().format("%H:%M:%S").to_string(),
    }
}

pub fn print_summaries(summaries: &VecDeque<BatchSummary>) {
    let len = summaries.len();
    let start = if len > 5 { len - 5 } else { 0 };

    clear_previous_lines(if len < 2 {0} else {12});

    println!("\nProcessing batch {}", &summaries[len-1].batch_number);

    println!("\n===== LAST 5 BATCHES =====");

    for i in 0..5 {
        if start + i < len {
            let s = &summaries[start + i];
            println!(
                "Batch {:>4} | Rows {:>4} | {:>4} ms | {}",
                s.batch_number,
                s.rows_read,
                s.elapsed_ms,
                s.timestamp,
            );
        } else {
            println!("Batch ---- | Rows ---- | ---- ms | --:--:--");
        }
    }

    println!("==========================\n");
    io::stdout().flush().unwrap();
}

fn clear_previous_lines(count: u8) {
    print!("\x1b[{}A", count);
    print!("\x1b[2K");
    io::stdout().flush().unwrap();
} 