from typing import Any, Optional

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from opensea_monitoring.utils.configs import settings


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
    return spark
