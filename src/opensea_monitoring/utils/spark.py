from typing import TYPE_CHECKING, Any, Optional

import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from opensea_monitoring.utils.configs import settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_spark_session(
    app_name: Optional[str] = settings.spark_app_name,
    master: Optional[str] = settings.spark_master,
    config: Optional[SparkConf | dict[str, Any]] = None,
) -> SparkSession:
    """
    Retrieves a Spark session with the provided configuration.
    If no configuration is provided, it will use the default configuration
    from the settings module.
    """
    if config is None:
        config = {}
    if isinstance(config, dict):
        config = SparkConf().setAll(list(config.items()))
    spark = (
        SparkSession.builder.appName(app_name)  # type: ignore
        .master(master)
        .config(conf=config)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    return spark


def write_df_to_kafka_topic(
    df: "DataFrame",
    topic: str,
    kafka_brokers: str,
    key_column: Optional[str] = None,
) -> None:
    """
    Writes a DataFrame to a Kafka topic.

    @param df: The DataFrame to write.
    @param topic: The Kafka topic to write to.
    @param kafka_brokers: The Kafka brokers to connect to.
    @param key_column: The column to use as the key. If None, no key will be used.
        The value column is a column with the JSON representation of the rest of the columns.
    """
    key_col = F.col(key_column) if key_column else F.lit(None)
    df_cols = [col for col in df.columns if col != key_column]
    value_col = F.to_json(
        F.struct(*df_cols), options={"ignoreNullFields": "false"}
    ).alias("value")
    df.select(key_col, value_col).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_brokers
    ).option("topic", topic).save()
