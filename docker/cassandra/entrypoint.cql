CREATE KEYSPACE IF NOT EXISTS opensea WITH REPLICATION = { 'class': 'SimpleStrategy',
'replication_factor': 1 };
USE opensea;
CREATE TABLE IF NOT EXISTS global_metrics (
    metric text,
    -- eg. "total_traded_volume_eur__24h", "top_collection_by_volume___30m"
    timestamp timestamp,
    --  eg. 2021-09-01 00:00:00, 2021-09-01 00:30:00
    value double,
    -- eg. 1000000, null
    collection text,
    -- eg. null, "CoolMonkeyCollection"
    PRIMARY KEY ((metric), timestamp)
) WITH CLUSTERING
ORDER BY (timestamp DESC);
CREATE TABLE IF NOT EXISTS collections_metrics (
    metric text,
    -- eg. 'total_traded_volume_eur__24h', 'total_traded_volume_eur__1m', 'collection_top
    timestamp timestamp,
    --  eg. 2021-09-01 00:00:00, 2021-09-01 00:30:00
    collection text,
    --  eg. 'CoolMonkeyCollection', 'MyAwesomeCollection', 'MyAwesomeCollection'
    value double,
    -- eg. 1000000, 12400
    asset_name text,
    asset_url text,
    image_url text,
    -- eg. null, "https://opensea.io/static/images/collections/cool-monkey-collection.png"
    PRIMARY KEY ((collection, metric), timestamp)
) WITH CLUSTERING
ORDER BY (timestamp DESC);