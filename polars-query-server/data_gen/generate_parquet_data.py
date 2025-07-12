import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

OUTPUT_DIR = Path(__file__).parent / ".." / "data"
OUTPUT_DIR.mkdir(exist_ok=True)

N_FILES = 5
ROWS_PER_FILE = 10_000_000

names = [f"user_{i}" for i in range(1000)]
cities = ["NY", "LA", "SF", "CHI", "HOU"]

for idx in range(N_FILES):
    df = pl.DataFrame({
        "name": np.random.choice(names, ROWS_PER_FILE),
        "age": np.random.randint(18, 80, ROWS_PER_FILE),
        "city": np.random.choice(cities, ROWS_PER_FILE),
        "signup_date": pl.date_range(datetime(2020,1,1), datetime(2021,1,1), interval="1d").sample(ROWS_PER_FILE, with_replacement=True),
        "balance": np.random.rand(ROWS_PER_FILE) * 10000,
    })
    file_path = OUTPUT_DIR / f"sample_{idx}.parquet"
    df.write_parquet(file_path)
    size_mb = file_path.stat().st_size / (1024*1024)
    print(f"{file_path}: {size_mb:.2f} MB")
