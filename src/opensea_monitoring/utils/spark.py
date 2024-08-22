from typing import Any, Optional

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from opensea_monitoring.utils.configs import settings


def get_spark_session(
    app_name: Optional[str] = settings.spark_app_name,
    master: Optional[str] = settings.spark_master,
    config: Optional[SparkConf | dict[str, Any]] = None,
    enable_eager_eval: bool = False,
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
    if settings.spark_max_cpu_cores and not config.get("spark.cores.max"):
        config.set("spark.executor.cores", str(settings.spark_max_cpu_cores))
    spark = (
        SparkSession.builder.appName(app_name)  # type: ignore
        .master(master)
        .config(conf=config)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    if enable_eager_eval:
        spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")
    return spark
