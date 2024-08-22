import polars as pl


class GlobalMetricsConfig:

    plots_config = {
        "transfers_count": {
            "type": "linear",
            "grain_options": ["1 minute", "1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Numero de transferencias",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_items_transferred": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Numero de items transferidos",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_sales": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Numero de ventas",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_usd_volume": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_eth_volume": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Volumen de ventas (ETH)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "top_collections_by_volume": {
            "type": "multilinear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Top colleciones por volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value", "hue": "collection"},
        },
    }

    df_schema = {
        "metric": pl.String(),
        "timestamp_at": pl.Datetime(),
        "value": pl.Float64(),
        "collection": pl.String(),
    }


class CollectionMetricsConfig:

    plots_config = {
        "total_items_transferred": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Numero de items transferidos",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_sales": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Numero de ventas",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_usd_volume": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_eth_volume": {
            "type": "linear",
            "grain_options": ["1 hour", "1 day"],
            "default_grain": "1 hour",
            "title": "Volumen de ventas (ETH)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
    }

    df_schema = {
        "collection": pl.String(),
        "metric": pl.String(),
        "timestamp_at": pl.Datetime(),
        "value": pl.Float64(),
    }
