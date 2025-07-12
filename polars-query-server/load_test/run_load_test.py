import asyncio
import httpx
import polars as pl
from statistics import mean
from time import perf_counter
from pathlib import Path

URL = "http://127.0.0.1:3000/run-query"
QUERIES = [
    "df = pl.read_parquet('data/sample_0.parquet')\ndf = df.select(['name','age'])"
] * 10

async def send_query(client, query):
    start = perf_counter()
    resp = await client.post(URL, content=query)
    duration = perf_counter() - start
    data = resp.json()
    size = len(str(data.get("output", "")))
    return {"duration": duration, "size": size, "cost": data.get("cost")}

async def main():
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[send_query(client, q) for q in QUERIES])

    df = pl.DataFrame(results)
    summary = df.select([
        pl.col("duration").min().alias("min"),
        pl.col("duration").mean().alias("avg"),
        pl.col("duration").quantile(0.95).alias("p95"),
        (pl.count() / df["duration"].sum()).alias("throughput")
    ])
    print(summary)
    df.write_csv(Path("load_test_summary.csv"))

if __name__ == "__main__":
    asyncio.run(main())
