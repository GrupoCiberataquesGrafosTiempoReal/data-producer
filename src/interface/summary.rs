use chrono::Local;

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

pub fn print_summary(summary: BatchSummary) {
    println!(
        "Batch {:>4} | Rows {:>4} | {:>4} ms | {}",
        summary.batch_number,
        summary.rows_read,
        summary.elapsed_ms,
        summary.timestamp,
    );
}