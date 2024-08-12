from unittest.mock import patch

import pytest

from opensea_monitoring.www.backend.client import OpenSeaDataMonitoringClient
from opensea_monitoring.www.backend.models import CollectionMetrics, GlobalMetrics


@pytest.fixture
def patched_client():
    with patch(
        "opensea_monitoring.www.backend.client.OpenSeaDataMonitoringClient.__init__"
    ) as mocked_client:
        mocked_client.return_value = None
        yield mocked_client


@pytest.fixture
def patched_cassandra_models():
    with patch(
        "cassandra.cqlengine.models.BaseModel.objects",
    ) as mocked_method:
        yield mocked_method


def test_global_metrics(patched_client, patched_cassandra_models):
    """
    Checks that the method works when both the metric and the grain are passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics()
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().filter.assert_not_called()
    GlobalMetrics.objects.all().limit.assert_not_called()


def test_global_metrics_metric_without_grain(patched_client):
    """
    Checks that an error is raised when the metric is passed without a grain.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_global_metrics(metric="volume")


def test_global_metrics_grain_without_metric(patched_client):
    """
    Checks that an error is raised when the metric is passed without a grain.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_global_metrics(grain="1 hour")


def test_global_metrics_grain_and_metric(patched_client, patched_cassandra_models):
    """
    Checks that the method works when both the metric and the grain are passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics(metric="volume", grain="1 hour")
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().filter.assert_called_with(metric="volume__1_hour")


def test_collection_metrics(patched_client, patched_cassandra_models):
    """
    Checks that the method works when both the metric and the grain are passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_collection_metrics()
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    CollectionMetrics.objects.all.assert_called_once()
    CollectionMetrics.objects.all().filter.assert_not_called()
    CollectionMetrics.objects.all().limit.assert_not_called()


def test_collection_metrics_collection_without_metric_grain(patched_client):
    """
    Checks that an error is raised when the collection is passed without a metric.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_collection_metrics(collection="collection")


def test_collection_metrics_collection_without_grain(patched_client):
    """
    Checks that an error is raised when the method is called with
    collection and metric but without a grain.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_collection_metrics(collection="collection", metric="volume")


def test_collection_metrics_collection_without_metric(patched_client):
    """
    Checks that an error is raised when the method is called with
    collection and grain but without a metric.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_collection_metrics(collection="collection", grain="1 hour")


def test_collection_metrics_without_collection(patched_client):
    """
    Checks that an error is raised when the method is called without a collection.
    """
    client = OpenSeaDataMonitoringClient()
    with pytest.raises(ValueError):
        client.get_collection_metrics(metric="volume", grain="1 hour")


def test_collections_metrics_filtering(patched_client, patched_cassandra_models):
    """
    Checks that the method works when all the filter parameters are passed
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_collection_metrics(
        collection="collection",
        metric="volume",
        grain="1 hour",
    )
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    CollectionMetrics.objects.all.assert_called_once()
    CollectionMetrics.objects.all().filter.assert_called_with(
        collection="collection", metric="volume__1_hour"
    )


def test_global_metrics_order_ascending(patched_client, patched_cassandra_models):
    """
    Checks that the method works when the order_ascending parameter is passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics(order_ascending=True)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().order_by.assert_called_with("timestamp_at")


def test_global_metrics_order_descending(patched_client, patched_cassandra_models):
    """
    Checks that the method works when the order_ascending parameter is passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics(order_ascending=False)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().order_by.assert_called_with("-timestamp_at")


def test_global_metrics_order_not_specified(patched_client, patched_cassandra_models):
    """
    Checks that the method works when the order_ascending parameter is passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics()
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().order_by.assert_not_called()


def test_global_metrics_with_limit(patched_client, patched_cassandra_models):
    """
    Checks that the method works when the limit parameter is passed.
    """
    client = OpenSeaDataMonitoringClient()
    result = client.get_global_metrics(limit=10)
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0
    GlobalMetrics.objects.all.assert_called_once()
    GlobalMetrics.objects.all().limit.assert_called_with(10)
