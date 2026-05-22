use std::fs;
use std::path::PathBuf;

pub fn list_supported_files(files_dir: String) -> Vec<PathBuf> {
    let mut files = vec![];

    if !PathBuf::from(&files_dir).is_dir() {
        println!("Directory does not exist or is not a directory");
        return files;
    }

    for entry in fs::read_dir(files_dir).unwrap() {
        let path = entry.unwrap().path();

        if let Some(ext) = path.extension() {
            let ext = ext.to_string_lossy().to_lowercase();

            if ext == "csv" || ext == "parquet" {
                files.push(path);
            }
        }
    }

    files.sort();

    files
}