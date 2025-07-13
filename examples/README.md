# Query Examples

This folder contains sample query plans that can be sent to the `polars-query-server` using the `/run-query` endpoint. Each file demonstrates a different feature of the query language.

- **basic_query.txt** – Reads a Parquet file and selects two columns.
- **groupby_query.txt** – Filters, groups by city and calculates the mean balance.
- **sort_query.txt** – Sorts the dataset by age.

Use `curl` or any HTTP client to POST the contents of these files to the running server.
