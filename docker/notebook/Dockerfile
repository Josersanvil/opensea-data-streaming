FROM jupyter/all-spark-notebook:spark-3.5.0

USER root

# Install AWS SDK dependencies to support S3 access (for MinIO)

RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    --output /usr/local/spark/jars/s3-2.18.41.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    --output /usr/local/spark/jars/aws-java-sdk-1.12.367.jar \
    && curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    --output /usr/local/spark/jars/hadoop-aws-3.3.2.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    --output /usr/local/spark/jars/aws-java-sdk-bundle-1.11.1026.jar
