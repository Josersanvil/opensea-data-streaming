# opensea-data-streaming

An scalable Python app that analyses NFT data from OpenSea in real time

## Setup

### Docker Compose

This Docker Compose file is used to set up a project with multiple services using Docker containers. It provides a convenient way to manage and deploy the project's dependencies.

#### Services

| Service Name         | Description                                                                 | Local Access                  |
|----------------------|-----------------------------------------------------------------------------|-------------------------------|
| notebook             | Jupyter Notebook server with Spark integration                               | <http://localhost:8888>        |
| spark                | Apache Spark master node                                                     | <http://localhost:8080>        |
| spark-worker         | Apache Spark worker node                                                     | -                             |
| minio                | Object storage server compatible with Amazon S3                               | <http://localhost:9000>        |
| cassandra            | Distributed NoSQL database                                                   | -                             |
| flink-jobmanager     | Apache Flink job manager                                                      | <http://localhost:8081>        |
| flink-taskmanager    | Apache Flink task manager                                                     | -                             |
| zookeeper            | Centralized service for maintaining configuration information                 | -                             |
| kafka                | Distributed streaming platform                                                | -                             |
| kafka-schema-registry| Centralized schema repository for Kafka                                       | -                             |
| kafka-rest-proxy     | RESTful proxy for Kafka                                                       | -                             |
| kafka-connect        | Distributed data integration framework for Kafka                              | -                             |
| ksqldb-server        | Streaming SQL engine for Apache Kafka                                         | <http://localhost:8088>        |
| conduktor-db         | PostgreSQL database for Conduktor                                             | -                             |
| conduktor            | Conduktor platform for managing Kafka clusters and topics                     | <http://localhost:8090>        |
