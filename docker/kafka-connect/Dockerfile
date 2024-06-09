FROM confluentinc/cp-kafka-connect:7.3.2

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest && \
    confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.3.3

COPY ./connectors/minio-s3-sink.json /etc/kafka-connect/minio-s3-sink.json
