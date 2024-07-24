import logging
from os import getenv
from typing import Optional


class AppSettings:
    APP_PREFIX = "OPENSEA_MONITORING"

    @property
    def logger(self) -> logging.Logger:
        logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        logger.setLevel(self.log_level)
        return logger

    @property
    def log_level(self) -> str:
        return getenv(f"{self.APP_PREFIX}_LOG_LEVEL", "WARNING")

    @property
    def spark_log_level(self) -> str:
        return getenv(f"{self.APP_PREFIX}_SPARK_LOG_LEVEL", getenv("SPARK_LOG_LEVEL", "WARN"))

    @property
    def spark_master(self) -> Optional[str]:
        return getenv(f"{self.APP_PREFIX}_SPARK_MASTER", "local[*]")

    @property
    def spark_app_name(self) -> str:
        return getenv(f"{self.APP_PREFIX}_SPARK_APP_NAME", "OpenSea Monitoring")

    @property
    def s3_bucket_raw_data(self) -> Optional[str]:
        return getenv(f"{self.APP_PREFIX}_S3_BUCKET_RAW_DATA")

    @property
    def s3_bucket_processed_data(self) -> Optional[str]:
        return getenv(f"{self.APP_PREFIX}_S3_BUCKET_PROCESSED_DATA")


settings = AppSettings()
