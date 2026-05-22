# data-producer

This program reads records from CSV or Parquet files and sends each in JSON format to an Apache Kafka broker on a specific topic.

## Run with Cargo

The program is compiled and executed using `cargo`. You can set up all needed tools following these instructions: https://rust-lang.org/tools/install/.

> It is necessary to install `cmake` to compile the project locally.

Compile the program:

```sh
cargo compile
```

Create an environment variables file named `.env` with this content:

```sh
KAFKA_BROKERS=<ip:port>     # Ex.: localhost:9092
KAFKA_TOPIC=<name>          # Ex.: topic.one
```

Run the program:
```sh
cargo run
```

## Run with Docker

Contaner is build and executed with Docker. You can follow these instructions to set up Docker: https://docs.docker.com/engine/install/.

Build the image:

```sh
docker build --no-cache -t data-producer .
```

Create an environment variables file named `.env` with this content:

```sh
KAFKA_BROKERS=<ip:port>     # Ex.: localhost:9092
KAFKA_TOPIC=<name>          # Ex.: topic.one
FILE_PATH=<path/to/file>    # Ex.: /opt/data/records.csv (Data file path inside container)
BATCH_SIZE=<numer>          # Ex.: 5 (Number of records per batch)
DELAY_MS=<number>           # Ex.: 2000 (Number of miliseconds to wait between batches)
```

Run the container:

> the example below assumes that the CSV/Parquet files are stored in `./data` (relative to the execution directory)

```sh
docker run \
    --network host \
    --env-file .env \
    -v ./data:/opt/data \
    --rm data-producer
```