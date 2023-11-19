# opensea-data-streaming

A scalable Python app that analyses NFT data from OpenSea in real time using open source technologies.

## Setup

### Docker Compose

#### Prerequisites

- [Docker Engine](https://docs.docker.com/engine/install/)

#### Usage

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

##### Apache Flink

[Apache Flink](https://flink.apache.org/) is a distributed stream processing framework.

Start Flink by using the profile `flink`:

| Service Name | Description | Local Access |
|--------------|-------------|--------------|
| flink-jobmanager | Apache Flink job manager | <http://localhost:8081> |
| flink-taskmanager | Apache Flink task manager | - |

> The local access for the job manager is the web UI for monitoring the cluster.

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
