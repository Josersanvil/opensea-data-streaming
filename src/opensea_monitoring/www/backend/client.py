from typing import TYPE_CHECKING, Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, dict_factory
from cassandra.cqlengine import connection

from opensea_monitoring.utils.configs import settings
from opensea_monitoring.www.backend.models import CollectionMetrics, GlobalMetrics

if TYPE_CHECKING:
    from datetime import datetime

    from cassandra.cluster import Session


class OpenSeaDataMonitoringClient:
    """
    Implements the main business logic for the client
    application that will be used to monitor the data
    from OpenSea.

    Retrieves the data from the Cassandra database
    populated by the data pipeline and performs
    the necessary transformations to return the
    data in the desired format.
    """

    def __init__(
        self,
        cassandra_host: Optional[str | list[str]] = None,
        cassandra_username: Optional[str] = None,
        cassandra_password: Optional[str] = None,
        cassandra_port: Optional[int] = None,
        default_keyspace: Optional[str] = None,
    ):
        self._cluster = self._get_cassandra_cluster(
            cassandra_host, cassandra_username, cassandra_password, cassandra_port
        )
        self._session = self._cluster.connect(
            default_keyspace or settings.cassandra_default_keyspace
        )
        self._session.row_factory = dict_factory
        connection.set_session(self._session)

    def __str__(self):
        return f"{self.__class__.__name__}({self._session.hosts})"

    def __repr__(self):
        return f"<{self.__str__()}>"

    @property
    def session(self) -> "Session":
        return self._session

    def get_global_metrics(
        self,
        metric: Optional[str] = None,
        grain: Optional[str] = None,
        from_ts: Optional["datetime"] = None,
        to_ts: Optional["datetime"] = None,
        limit: Optional[int] = None,
        order_ascending: Optional[bool] = None,
    ) -> list[GlobalMetrics]:
        """
        Retrieves the global metrics from the Cassandra database.
        You can filter the results by metric, grain, and timestamp range.

        Note that for any query, both metric and grain must be provided.

        @param metric: The metric to filter by.
        @param grain: The grain to filter by.
        @param from_ts: The start timestamp to filter by.
        @param to_ts: The end timestamp to filter by.
        @param limit: The maximum number of results to return.
        @param order_ascending: Use this to specify the order of the results.
            If True, the results will be sorted in ascending order by timestamp.
            If False, the results will be sorted in descending order by timestamp.
            Default is None, which means the results will be returned in the order
            they were inserted in the database.
        @return: A list of GlobalMetrics objects.
        """
        if bool(metric) ^ bool(grain):
            # metric and grain must be filtered together
            raise ValueError("Both metric and grain must be provided or neither.")
        global_metrics = GlobalMetrics.objects.all()
        if metric and grain:
            grain = "_".join(grain.split())
            metric_name = f"{metric}__{grain}"
            global_metrics = global_metrics.filter(metric=metric_name)
        if from_ts:
            global_metrics = global_metrics.filter(timestamp_at__gte=from_ts)
        if to_ts:
            global_metrics = global_metrics.filter(timestamp_at__lte=to_ts)
        if limit:
            global_metrics = global_metrics.limit(limit)
        if order_ascending is not None:
            if order_ascending:
                global_metrics = global_metrics.order_by("timestamp_at")
            else:
                global_metrics = global_metrics.order_by("-timestamp_at")
        return list(global_metrics)

    def get_collection_metrics(
        self,
        collection: Optional[str] = None,
        metric: Optional[str] = None,
        grain: Optional[str] = None,
        from_ts: Optional["datetime"] = None,
        to_ts: Optional["datetime"] = None,
        limit: Optional[int] = None,
        order_ascending: Optional[bool] = None,
    ) -> list[CollectionMetrics]:
        """
        Retrieves specific collection metrics from the Cassandra database.
        You can filter the results by collection, metric, grain, and timestamp range.

        Note that for any query, both collection, metric, and grain must be provided.

        @param collection: The collection to filter by.
        @param metric: The metric to filter by.
        @param grain: The grain to filter by.
        @param from_ts: The start timestamp to filter by.
        @param to_ts: The end timestamp to filter by.
        @param limit: The maximum number of results to return.
        @param ascending: If True, the results will be sorted in ascending
            order by timestamp.
        @return: A list of CollectionMetrics objects.
        """
        if bool(metric) ^ bool(grain):
            raise ValueError("Both metric and grain must be provided or neither.")
        metric_grain_provided = bool(metric) and bool(grain)
        if bool(metric_grain_provided) ^ bool(collection):
            raise ValueError("Collection must be provided when filtering")
        collection_metrics = CollectionMetrics.objects.all()
        if collection and metric and grain:
            # metric, grain, and collection must be filtered together
            grain = "_".join(grain.split())
            metric_name = f"{metric}__{grain}"
            collection_metrics = collection_metrics.filter(
                collection=collection, metric=metric_name
            )
        if from_ts:
            collection_metrics = collection_metrics.filter(timestamp_at__gte=from_ts)
        if to_ts:
            collection_metrics = collection_metrics.filter(timestamp_at__lte=to_ts)
        if limit:
            collection_metrics = collection_metrics.limit(limit)
        if order_ascending is not None:
            if order_ascending:
                collection_metrics = collection_metrics.order_by("timestamp_at")
            else:
                collection_metrics = collection_metrics.order_by("-timestamp_at")
        return list(collection_metrics)

    def _get_cassandra_cluster(
        self,
        cassandra_host: Optional[str | list[str]] = None,
        cassandra_username: Optional[str] = None,
        cassandra_password: Optional[str] = None,
        cassandra_port: Optional[int] = None,
    ) -> Cluster:
        """
        Retrieves a Cassandra cluster object with the provided configuration.
        """
        auth_provider = PlainTextAuthProvider(
            username=cassandra_username or settings.cassandra_username,
            password=cassandra_password or settings.cassandra_password,
        )
        host = cassandra_host or settings.cassandra_host
        if isinstance(host, str):
            host = [host]
        port = cassandra_port or settings.cassandra_port
        cluster = Cluster(
            host,
            port=port,
            auth_provider=auth_provider,
        )
        return cluster
