# opensea-data-streaming

A scalable Python app that analyses NFT data from OpenSea in real time using open source technologies.

See: [OpenSea Stream API Overview](https://docs.opensea.io/reference/stream-api-overview)

## Setup

### Docker Compose

#### Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/)

#### Instructions

Run the following command to start the core services:

```bash
docker compose up -d
```

This will start the following core services:

| Service Name | Description | Local Access |
|--------------|-------------|--------------|
| notebook | [Jupyter Notebook](https://jupyter.org/) server with Spark integration | <http://localhost:8888> |
| spark | [Apache Spark](https://spark.apache.org/) master node | <http://localhost:8080> |
| spark-worker | Apache Spark worker nodes. 2 replicas are started by default | - |
| [minio](https://min.io/) | An object storage server compatible with Amazon S3 | <http://localhost:9000> |

> The local access for spark is the web UI for monitoring the cluster.
> For minio, the local access is the web UI for managing the object storage.

All the services are configured to use the same network. This allows them to communicate with each other using their service names.
For example, the notebook server can access the Spark master node using the URL `spark:7077`.

Spark is configured to use the MinIO server as object storage. This allows the Spark jobs to read and write data to it using an URI like `s3a://<bucket>/<path>`. This is preffered over using the local filesystem as it allows the data to be persisted even if the Spark cluster is restarted.

To stop the services run:

```bash
docker compose --profile all down
```

> Use the `down -v` option to delete all the data of the application as well.

#### Additional Services

You can add additional services to the deployment using the `--profile` option:

For example, to start the core services + Kafka and Cassandra run:

```bash
docker compose --profile kafka --profile cassandra up -d
```

You can also start all services by using the `all` profile:

```bash
docker compose --profile all up -d
```

##### Apache Cassandra

[Cassandra](https://cassandra.apache.org/_/index.html) is a distributed NoSQL database optimized for handling large amounts of data. The data is stored in a columnar format so that it can be queried efficiently for analytics. It can be used to store the data generated by the application.

Start cassandra by using the profile `cassandra`:

| Service Name | Description | Local Access |
|--------------|-------------|--------------|
| cassandra | Distributed NoSQL database | <http://localhost:9042> |

> The local access can be used to access the Cassandra Query Language (CQL) shell, which allows you interact with the database using a compatible client.

##### Apache Kafka

[Kafka](https://kafka.apache.org/) is a distributed events processing platform that can be used to store and process the data generated by the stream.

Start Kafka by using the profile `kafka`:

| Service Name | Description | Local Access |
|--------------|-------------|--------------|
| [zookeeper](https://zookeeper.apache.org/) | Centralized service for maintaining configuration information | - |
| kafka | Distributed streaming platform | - |
| kafka-schema-registry| Centralized schema repository for Kafka | - |
| kafka-rest-proxy | RESTful proxy for Kafka | - |
| kafka-connect | Distributed data integration framework for Kafka | - |
| ksqldb-server | Streaming SQL engine for Apache Kafka | - |
| [conduktor](https://docs.conduktor.io/platform/) | Web console for managing Kafka clusters and topics | <http://localhost:8090> |
| conduktor-db | PostgreSQL database for Conduktor | - |

## Usage

The core services are designed to start collecting data from the OpenSea API in real time as soon as they are started.

To start processing the data in batch or stream mode you can use the Python script in the `opensea_monitoring.cli` package.

### Commands

When using the Docker Compose setup, you can execute the following commands from the `spark` service to start processing the data from the OpenSea API.

Depending on the execution mode, you can process the data in batch or stream mode. In general the script will read the raw events gotten from the OpenSea WebSocket and write the aggregated data to the enriched events Kafka topic.

#### Get help

Use the following command to see all the available options of the script:

```bash
docker compose --profile kafka exec -it spark python -m opensea_monitoring.cli --help
```

#### Process global events in Batch mode

This command will process the global events in batch mode. It will read the raw events retrieved from the OpenSea API in an S3 bucket and write the enriched events to the Kafka topic `OpenSeaEnrichedGlobalEvents`. The data will be aggregated in 1 hour windows.

```sh
docker compose --profile kafka exec -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 -it spark \
    python -m opensea_monitoring.cli global '1 hour' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    -l INFO
```

#### Process collection events in Batch mode

```bash
docker compose --profile kafka exec -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 -it spark \
    python -m opensea_monitoring.cli collections '1 hour' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    -l INFO
```

When processing events in Batch mode, you can also filter the events by a timestamp range. eg:

```bash
docker compose --profile kafka exec -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 -it spark \
    python -m opensea_monitoring.cli global '1 hour' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    --timestamp-start '2024-07-28' \
    --timestamp-end '2024-07-28T12:00:00' \
    -l INFO
```

#### Process global events in Stream mode

This command will process the global events in stream mode. It will read a continuous stream of the raw events retrieved from the OpenSea API from the "Raw Events" Kafka Topic and write the enriched events to the Kafka topic `OpenSeaEnrichedGlobalEvents`. The data will be aggregated in 1 minute windows.

```bash
docker compose --profile kafka exec -it \
    -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 \
    -e OPENSEA_MONITORING_LOG_LEVEL=INFO \
    spark \
    python -m opensea_monitoring.cli global '1 minute' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    --slide-duration '1 minute' \
    --watermark-duration '1 minute' \
    --checkpoint-dir s3a://processed-data/checkpoints/topics/OpenSeaEnrichedEvents/ \
    -l INFO
```

#### Process collection events in Stream mode

```bash
docker compose --profile kafka exec -it \
    -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 \
    -e OPENSEA_MONITORING_LOG_LEVEL=INFO \
    spark \
    python -m opensea_monitoring.cli collections '1 minute' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    --slide-duration '1 minute' \
    --watermark-duration '1 minute' \
    --checkpoint-dir s3a://processed-data/checkpoints/topics/OpenSeaEnrichedEvents/
```

#### Debug mode for streams

Use the `--debug` option to enable debug mode for the stream processing. This will write the results of the stream processing to the console instead of writing them to the Kafka topic.

```bash
docker compose --profile kafka exec -it \
    -e OPENSEA_MONITORING_SPARK_MASTER=spark://spark:7077 \
    -e OPENSEA_MONITORING_LOG_LEVEL=DEBUG \
    spark \
    python -m opensea_monitoring.cli global '1 minute' \
    --raw-events-s3-uri s3a://raw-data/topics/OpenSeaRawEvents \
    --raw-events-kafka-topic OpenSeaRawEvents \
    --kafka-brokers kafka:19092 \
    --kafka-topic OpenSeaEnrichedGlobalEvents \
    --slide-duration '1 minute' \
    --checkpoint-dir s3a://processed-data/checkpoints/topics/OpenSeaEnrichedEvents/ \
    --debug
```
