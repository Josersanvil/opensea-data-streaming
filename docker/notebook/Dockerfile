FROM jupyter/all-spark-notebook:spark-3.5.0

# Copy spark-defaults.conf
COPY ./conf/spark-defaults.conf /usr/local/spark/conf/spark-defaults.conf

# Install SDKs
USER root

ENV SPARK_SDKS_DIR=/usr/local/spark/jars
COPY ./conf/install_jars.sh /tmp/install_jars.sh
RUN chmod +x /tmp/install_jars.sh && /tmp/install_jars.sh -o $SPARK_SDKS_DIR
