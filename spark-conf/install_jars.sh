#! /bin/bash

# Parse command line arguments
while getopts ":o:" opt; do
    case $opt in
        o)
            # Set the output directory for the JARs
            ouput_dir=$OPTARG
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

if [ -z "$ouput_dir" ]; then
    echo "Output directory not specified. Exiting."
    exit 1
fi

# Install JARs for the AWS SDK to support S3 access (for MinIO)
echo "Installing AWS SDK JARs to ${ouput_dir} ..."
curl https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    --output ${ouput_dir}/s3-2.18.41.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    --output ${ouput_dir}/aws-java-sdk-1.12.367.jar \
    && curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    --output ${ouput_dir}/hadoop-aws-3.3.2.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    --output ${ouput_dir}/aws-java-sdk-bundle-1.11.1026.jar

# Install Kafka Spark Streaming SDKs
echo "Installing Spark Streaming Kafka SDK JARs to ${ouput_dir} ..."
curl https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.0/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar \
    --output ${ouput_dir}/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar \
    && curl https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    --output ${ouput_dir}/spark-sql-kafka-0-10_2.12-3.5.0.jar
