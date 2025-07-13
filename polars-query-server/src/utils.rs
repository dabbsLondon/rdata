use polars::prelude::*;
use std::fs::File;
use std::io::{self, Cursor};

/// Compressed bytes or path to saved Feather file.
pub struct PreparedOutput {
    pub bytes: Option<Vec<u8>>, // zstd compressed
    pub path: Option<String>,
}

/// Compress a DataFrame using zstd after writing as IPC (Feather).
fn compress_df(df: &DataFrame) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut df = df.clone();
    IpcWriter::new(&mut buf)
        .finish(&mut df)
        .map_err(|e| io::Error::other(e.to_string()))?;
    zstd::encode_all(Cursor::new(buf), 0)
}

/// Save a DataFrame to the given path in Feather format.
fn save_df(path: &str, df: &DataFrame) -> io::Result<()> {
    let file = File::create(path)?;
    let mut df = df.clone();
    IpcWriter::new(file)
        .finish(&mut df)
        .map_err(|e| io::Error::other(e.to_string()))
}

/// Prepare output either inline (<1MB) or as file on disk.
pub fn prepare_output(id: u64, df: &DataFrame) -> io::Result<PreparedOutput> {
    let compressed = compress_df(df)?;
    if compressed.len() <= 1_000_000 {
        Ok(PreparedOutput {
            bytes: Some(compressed),
            path: None,
        })
    } else {
        let path = format!("output_{}.feather", id);
        save_df(&path, df)?;
        Ok(PreparedOutput {
            bytes: None,
            path: Some(path),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn small_dataframe_inline() {
        let df = df!["val" => [1, 2, 3]].unwrap();
        let out = prepare_output(1, &df).unwrap();
        assert!(out.bytes.is_some());
        assert!(out.path.is_none());
    }

    #[test]
    fn large_dataframe_as_file() {
        let data: Vec<i32> = (0..5_000_000).collect();
        let df = df!["val" => &data].unwrap();
        let out = prepare_output(2, &df).unwrap();
        assert!(out.bytes.is_none());
        assert!(out.path.is_some());
        let path = out.path.unwrap();
        assert!(fs::metadata(&path).is_ok());
        fs::remove_file(path).unwrap();
    }
}
