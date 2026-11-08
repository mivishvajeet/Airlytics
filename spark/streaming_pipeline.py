"""Local PySpark Structured Streaming pipeline for flight telemetry.

The active local sink is Parquet. No cloud credentials or cloud connectors are
required. Kafka offsets and Spark state are recovered from the local checkpoint.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import (
        avg,
        col,
        concat,
        count,
        first,
        from_json,
        max as spark_max,
        md5,
        min as spark_min,
        round as spark_round,
        session_window,
        to_timestamp,
        unix_timestamp,
    )
    from pyspark.sql.streaming import StreamingQuery
    from pyspark.sql.types import StructType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore
    DataFrame = None  # type: ignore
    StructType = None  # type: ignore

from spark.schemas import FLIGHT_PING_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("airlytics-spark")


def get_spark_session(app_name: str = "Airlytics-Local-Streaming") -> SparkSession:
    """Create a local Spark session; Kafka connector is supplied to spark-submit."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required. Install spark/requirements.txt.")
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def parse_and_watermark_pings(
    raw_stream_df: DataFrame,
    watermark_delay: str = "10 minutes",
) -> DataFrame:
    """Parse Kafka JSON values and apply an event-time watermark."""
    parsed = raw_stream_df.select(
        from_json(col("value").cast("string"), FLIGHT_PING_SCHEMA).alias("ping")
    ).select("ping.*")
    timestamped = parsed.withColumn("event_time", to_timestamp(col("event_timestamp")))
    return timestamped.withWatermark("event_time", watermark_delay)


def sessionize_flight_legs(
    watermarked_df: DataFrame,
    gap_duration: str = "15 minutes",
) -> DataFrame:
    """Build aircraft sessions using a 15-minute inactivity gap."""
    return (
        watermarked_df
        .groupBy(
            col("icao24"),
            session_window(col("event_time"), gap_duration).alias("session"),
        )
        .agg(
            first("callsign", ignorenulls=True).alias("callsign"),
            first("origin_country", ignorenulls=True).alias("origin_country"),
            spark_min("event_time").alias("session_start"),
            spark_max("event_time").alias("session_end"),
            count("*").alias("ping_count"),
            spark_max("baro_altitude").alias("max_altitude_m"),
            spark_round(avg("velocity"), 1).alias("avg_velocity_ms"),
        )
        .withColumn(
            "duration_minutes",
            spark_round(
                (unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))) / 60.0,
                1,
            ),
        )
        .withColumn(
            "flight_leg_id",
            md5(concat(col("icao24"), col("session.start").cast("string"))),
        )
        .select(
            "flight_leg_id",
            "icao24",
            "callsign",
            "origin_country",
            "session_start",
            "session_end",
            "duration_minutes",
            "ping_count",
            "max_altitude_m",
            "avg_velocity_ms",
        )
    )


def start_parquet_query(
    df: DataFrame,
    output_path: str,
    checkpoint_dir: str,
) -> StreamingQuery:
    """Write completed sessions to local Parquet in append mode."""
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .option("compression", "snappy")
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Airlytics local Spark streaming pipeline")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "raw-flight-pings"))
    parser.add_argument("--output-path", "--parquet-path", dest="output_path",
                        default=os.getenv("PARQUET_PATH", "data/bronze/flight-legs"))
    parser.add_argument("--checkpoint-dir",
                        default=os.getenv("CHECKPOINT_DIR", "data/checkpoints/flight-legs"))
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    args = parser.parse_args()

    if not PYSPARK_AVAILABLE:
        logger.error("PySpark is required. Install with: pip install -r spark/requirements.txt")
        sys.exit(1)

    spark = get_spark_session()
    logger.info("Reading Kafka topic %s at %s", args.topic, args.kafka_bootstrap)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    legs = sessionize_flight_legs(parse_and_watermark_pings(raw))
    query = start_parquet_query(legs, args.output_path, args.checkpoint_dir)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stream gracefully...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
