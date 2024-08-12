from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

__all__ = ["GlobalMetrics", "CollectionMetrics"]


class GlobalMetrics(Model):
    """
    The model is meant to be used as a read-only model to retrieve the
    global metrics for the OpenSea data.

    Primary key (composite key): metric, timestamp
    Partition Key: metric
    Clustering Key: timestamp (DESC)
    """

    __table_name__ = "global_metrics"

    metric = columns.Text(primary_key=True)
    timestamp_at = columns.DateTime(
        primary_key=True, clustering_order="DESC", db_field="timestamp"
    )
    value = columns.Double()
    collection = columns.Text()


class CollectionMetrics(Model):
    """
    The model is meant to be used as a read-only model to retrieve the
    collection metrics for the OpenSea data.

    Primary key (composite key): (collection, metric), timestamp
    Partition Key: collection, metric
    Clustering Key: timestamp (DESC)
    """

    __table_name__ = "collections_metrics"

    collection = columns.Text(primary_key=True)
    metric = columns.Text(primary_key=True)
    timestamp_at = columns.DateTime(
        primary_key=True, clustering_order="DESC", db_field="timestamp"
    )
    value = columns.Double()
    asset_name = columns.Text()
    asset_url = columns.Text()
    image_url = columns.Text()
