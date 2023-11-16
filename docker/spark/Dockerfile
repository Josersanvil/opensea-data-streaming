FROM docker.io/bitnami/spark:3.5

USER root
# Install prerequisites
RUN apt-get update && apt-get install -y curl

USER 1001

# Install AWS SDK
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar \
    --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar

RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    --output /opt/bitnami/spark/jars/s3-2.18.41.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    --output /opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar
