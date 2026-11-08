"""PySpark StructType schemas for streaming telemetry and curated marts."""

from typing import Any
try:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    StructType = None  # type: ignore

if PYSPARK_AVAILABLE:
    # Schema for raw JSON pings consumed from Kafka topic 'raw-flight-pings'
    FLIGHT_PING_SCHEMA = StructType([
        StructField("message_id", StringType(), False),
        StructField("ingest_timestamp", StringType(), False),
        StructField("event_timestamp", StringType(), False),
        StructField("event_time_epoch", LongType(), False),
        StructField("icao24", StringType(), False),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("longitude", DoubleType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), False),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True),
    ])

    # Schema for sessionized flight legs output
    FLIGHT_LEG_SCHEMA = StructType([
        StructField("flight_leg_id", StringType(), False),
        StructField("icao24", StringType(), False),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("session_start", TimestampType(), False),
        StructField("session_end", TimestampType(), False),
        StructField("duration_minutes", DoubleType(), True),
        StructField("ping_count", LongType(), False),
        StructField("max_altitude_m", DoubleType(), True),
        StructField("avg_velocity_ms", DoubleType(), True),
    ])
else:
    FLIGHT_PING_SCHEMA = None  # type: ignore
    FLIGHT_LEG_SCHEMA = None   # type: ignore

