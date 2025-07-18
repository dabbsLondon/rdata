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

    let dir = std::env::var("METRICS_DIR").unwrap_or_else(|_| "metrics".into());
    let path = Path::new(&dir).join("query_metrics.parquet");
    let mut df_to_write = if path.exists() {
        let file = File::open(&path)?;
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

    let file = File::create(&path)?;
    ParquetWriter::new(file)
        .finish(&mut df_to_write)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn record_and_append_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        std::env::set_var("METRICS_DIR", tmp.path());
        let path = tmp.path().join("query_metrics.parquet");

        record_metrics("q1", 10, 5, 100).unwrap();
        assert!(path.exists());
        let file = File::open(&path).unwrap();
        let df = ParquetReader::new(file).finish().unwrap();
        assert_eq!(df.height(), 1);

        record_metrics("q2", 20, 6, 200).unwrap();
        let file2 = File::open(&path).unwrap();
        let df2 = ParquetReader::new(file2).finish().unwrap();
        assert_eq!(df2.height(), 2);
        std::env::remove_var("METRICS_DIR");
    }

    #[test]
    #[serial]
    fn record_metrics_default_dir() {
        let dir = tempfile::tempdir().unwrap();
        let current = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        std::env::remove_var("METRICS_DIR");

        record_metrics("q", 1, 1, 1).unwrap();
        assert!(std::path::Path::new("metrics/query_metrics.parquet").exists());

        std::env::set_current_dir(current).unwrap();
    }

    #[test]
    #[serial]
    fn record_metrics_handles_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("METRICS_DIR", dir.path());
        let path = dir.path().join("query_metrics.parquet");
        std::fs::write(&path, b"not parquet").unwrap();
        assert!(record_metrics("q", 1, 1, 1).is_err());
        std::env::remove_var("METRICS_DIR");
    }
}
