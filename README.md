# rdata

`rdata` contains **polars-query-server**, a minimal HTTP service written in Rust for executing [Polars](https://pola.rs/) queries. The server accepts textual query plans and returns the resulting data either inline (compressed) or as a Feather file on disk.

## Running the Server

```bash
cd polars-query-server
cargo run
```

The server listens on `127.0.0.1:3000` and exposes a single `POST /run-query` endpoint.

### Example Query

Save the following to `query.txt`:

```text
df = pl.read_parquet("data/sample_0.parquet")
df = df.filter(pl.col("age") > 30)
df = df.groupby("city").agg(pl.col("balance").mean())
```

Send it using `curl`:

```bash
curl -X POST http://127.0.0.1:3000/run-query -d @query.txt
```

A Python example using `httpx`:

```python
import httpx
query = """
df = pl.read_parquet('data/sample_0.parquet')
df = df.select(['name','age'])
"""
resp = httpx.post('http://127.0.0.1:3000/run-query', content=query)
print(resp.json())
```

## Generating Sample Data

Large Parquet files for testing can be generated with:

```bash
cd polars-query-server/data_gen
python3 generate_parquet_data.py
```

Files are created under `polars-query-server/data/`.

## Load Testing

Once the server is running and data is generated you can execute a simple load test:

```bash
cd polars-query-server/load_test
python3 run_load_test.py
```

A CSV summary will be written to `load_test_summary.csv`.

## Running the Tests

All unit and integration tests can be executed with:

```bash
cd polars-query-server
cargo test
```

### Code Coverage

The project uses [cargo-tarpaulin](https://github.com/xd009642/tarpaulin) for coverage. Install it once and run:

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --workspace --timeout 120
```

The coverage percentage will be printed at the end of the run and is also reported in the CI workflow summary.
