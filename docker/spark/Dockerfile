FROM docker.io/bitnami/spark:3.5


USER root
# Install prerequisites
RUN apt-get update && apt-get install -y curl gcc

# Install SDKs
ENV SPARK_SDKS_DIR=/opt/bitnami/spark/jars
COPY ./conf/install_jars.sh /tmp/install_jars.sh
RUN chmod +x /tmp/install_jars.sh && /tmp/install_jars.sh -o $SPARK_SDKS_DIR

# Install Jupyter
RUN pip install jupyterlab py4j

# Copy spark-defaults.conf
COPY ./conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf


WORKDIR /app
# ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root"]
