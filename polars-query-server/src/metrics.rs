use polars::prelude::*;
use std::fs::File;
use std::io::Result as IoResult;
use std::path::Path;

/// Append a single metric row to `metrics/query_metrics.parquet`.
///
/// If the file already exists it will be loaded, the row appended and then
/// written back. Otherwise a new file is created.
pub fn record_metrics(
    query: &str,
    duration_ms: u128,
    cost: usize,
    output_size: u64,
) -> IoResult<()> {
    let mut df = df![
        "query" => [query.to_string()],
        "duration_ms" => [duration_ms as i64],
        "cost" => [cost as i64],
        "output_size" => [output_size as i64]
    ]
    .map_err(|e| std::io::Error::other(e.to_string()))?;

    let path = Path::new("metrics/query_metrics.parquet");
    let mut df_to_write = if path.exists() {
        let file = File::open(path)?;
        let mut existing = ParquetReader::new(file)
            .finish()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        existing
            .vstack_mut(&mut df)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        existing
    } else {
        df
    };

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    ParquetWriter::new(file)
        .finish(&mut df_to_write)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}
