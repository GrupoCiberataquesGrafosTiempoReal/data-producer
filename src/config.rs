#[derive(Clone)]
pub struct Config {
    pub file_index: usize,
    pub batch_size: usize,
    pub delay_ms: u64,
}