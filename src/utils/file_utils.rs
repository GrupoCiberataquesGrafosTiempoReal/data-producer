use anyhow::Result;
use std::fs;
use std::path::PathBuf;

pub fn list_supported_files(folder: &str) -> Result<Vec<PathBuf>> {
    let mut files = vec![];

    for entry in fs::read_dir(folder)? {
        let entry = entry?;
        let path = entry.path();

        if let Some(ext) = path.extension() {
            let ext = ext.to_string_lossy().to_lowercase();

            if ext == "csv" || ext == "parquet" {
                files.push(path);
            }
        }
    }

    files.sort();

    Ok(files)
}