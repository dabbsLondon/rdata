FROM rust:1.74 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:buster-slim
WORKDIR /app
COPY --from=builder /app/target/release/polars-query-server /usr/local/bin/polars-query-server
CMD ["polars-query-server"]
