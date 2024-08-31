import pyspark.sql.types as T


def get_opensea_raw_events_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("event", T.StringType(), False),
            T.StructField("topic", T.StringType(), True),
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField("event_type", T.StringType(), True),
                        T.StructField("sent_at", T.StringType(), True),
                        T.StructField("status", T.StringType(), True),
                        T.StructField(
                            "payload",
                            T.StructType(
                                [
                                    T.StructField("quantity", T.LongType(), True),
                                    T.StructField("listing_date", T.StringType(), True),
                                    T.StructField("listing_type", T.StringType(), True),
                                    T.StructField(
                                        "collection",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "slug", T.StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    T.StructField(
                                        "item",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "metadata",
                                                    T.StructType(
                                                        [
                                                            T.StructField(
                                                                "name",
                                                                T.StringType(),
                                                                True,
                                                            ),
                                                            T.StructField(
                                                                "image_url",
                                                                T.StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                T.StructField(
                                                    "permalink", T.StringType(), True
                                                ),
                                                T.StructField(
                                                    "nft_id", T.StringType(), True
                                                ),
                                                T.StructField(
                                                    "chain",
                                                    T.StructType(
                                                        [
                                                            T.StructField(
                                                                "name",
                                                                T.StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    T.StructField(
                                        "from_account",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "address", T.StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    T.StructField(
                                        "to_account",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "address", T.StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    T.StructField(
                                        "payment_token",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "symbol", T.StringType(), True
                                                ),
                                                T.StructField(
                                                    "eth_price", T.StringType(), True
                                                ),
                                                T.StructField(
                                                    "usd_price", T.StringType(), True
                                                ),
                                            ]
                                        ),
                                    ),
                                ],
                            ),
                            True,
                        ),
                    ]
                ),
                False,
            ),
        ]
    )
