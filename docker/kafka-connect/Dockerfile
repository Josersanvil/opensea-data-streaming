FROM confluentinc/cp-kafka-connect:7.3.2

RUN curl -O -L "https://downloads.datastax.com/kafka/kafka-connect-cassandra-sink.tar.gz" \
    && mkdir datastax-connector \
    && tar xzf kafka-connect-cassandra-sink.tar.gz -C datastax-connector --strip-components=1 \
    && mv datastax-connector/kafka-connect* datastax-connector/kafka-connect-cassandra.jar
