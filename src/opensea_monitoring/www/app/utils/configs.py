import polars as pl


class AppConfig:

    grain_options = {
        "options": {
            "30 segundos": {
                "grain_value": "30 seconds",
            },
            "1 minuto": {
                "grain_value": "1 minute",
            },
            "5 minutos": {"grain_value": "5 minutes"},
            "1 hora": {"grain_value": "1 hour"},
            "1 dia": {"grain_value": "1 day"},
        },
        "default_value": "5 minutos",
    }
    refresh_rate_options = {
        "options": {
            "5 segundos": {"value_secs": 5},
            "30 segundos": {"value_secs": 30},
            "1 minuto": {"value_secs": 60},
        },
        "default_value": "5 segundos",
    }


class GlobalMetricsConfig:

    plots_config = {
        "total_transfers": {
            "type": "linear",
            "title": "Numero de transferencias",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_items_transferred": {
            "type": "linear",
            "title": "Numero de items transferidos",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_sales": {
            "type": "linear",
            "title": "Numero de ventas",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_usd_volume": {
            "type": "linear",
            "title": "Volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_eth_volume": {
            "type": "linear",
            "title": "Volumen de ventas (ETH)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "top_collections_by_sales_volume_usd_price": {
            "type": "multilinear",
            "default_grain": "1 hour",
            "title": "Top colecciones por volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value", "hue": "collection"},
        },
        "top_collections_by_transfers_volume_transfers_count": {
            "type": "multilinear",
            "default_grain": "1 hour",
            "title": "Top colecciones por volumen de transacciones (Total)",  # noqa
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
        "collection_total_items_transferred": {
            "type": "linear",
            "default_grain": "1 hour",
            "title": "Numero de items transferidos",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "collection_total_transfers": {
            "type": "linear",
            "title": "Numero de transacciones",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_usd_volume": {
            "type": "linear",
            "title": "Volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "total_eth_volume": {
            "type": "linear",
            "title": "Volumen de ventas (ETH)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "collection_top_assets_by_transfers": {
            "type": "multilinear",
            "title": "Top Assets por volumen de transferencias",
            "axes": {"x": "timestamp_at", "y": "value", "hue": "asset_name"},
        },
        "collection_total_usd_volume": {
            "type": "linear",
            "title": "Volumen de ventas (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
        "floor_assets_usd_price": {
            "type": "linear",
            "title": "Precio suelo de los activos (USD)",
            "axes": {"x": "timestamp_at", "y": "value"},
        },
    }

    df_schema = {
        "collection": pl.String(),
        "metric": pl.String(),
        "timestamp_at": pl.Datetime(),
        "value": pl.Float64(),
        "asset_name": pl.String(),
        "asset_url": pl.String(),
        "image_url": pl.String(),
    }
