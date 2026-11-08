"""PySpark Structured Streaming pipeline for flight telemetry and sessionization."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any, Optional

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
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None  # type: ignore
    DataFrame = None     # type: ignore

from spark.schemas import FLIGHT_PING_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("airlytics-spark")


def get_spark_session(
    app_name: str = "Airlytics-Streaming",
    master: Optional[str] = None,
    gcp_project_id: Optional[str] = None,
) -> SparkSession:
    """Initialize or retrieve a SparkSession configured with Kafka and BigQuery connectors."""
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # Standard Kafka and BigQuery connector coordinates
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
    ]
    builder = builder.config("spark.jars.packages", ",".join(packages))
    builder = builder.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    builder = builder.config("spark.sql.shuffle.partitions", "4")

    if gcp_project_id:
        builder = builder.config("spark.datasource.bigquery.projectId", gcp_project_id)

    return builder.getOrCreate()


def parse_and_watermark_pings(raw_stream_df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    """Deserialize Kafka raw JSON payloads, cast timestamps, and set watermark."""
    # Handle both direct Kafka format (with key/value columns) and test DataFrames
    if "value" in raw_stream_df.columns:
        parsed_df = raw_stream_df.select(
            from_json(col("value").cast("string"), FLIGHT_PING_SCHEMA).alias("ping")
        ).select("ping.*")
    else:
        parsed_df = raw_stream_df

    # Cast event_timestamp string to TimestampType
    timestamped_df = parsed_df.withColumn(
        "event_time", to_timestamp(col("event_timestamp"))
    )

    # Watermark handles late-arriving telemetry up to watermark_delay
    return timestamped_df.withWatermark("event_time", watermark_delay)


def sessionize_flight_legs(watermarked_df: DataFrame, gap_duration: str = "15 minutes") -> DataFrame:
    """Aggregate individual position pings into discrete flight legs using session windowing."""
    return (
        watermarked_df.groupBy(
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
            spark_round((unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))) / 60.0, 1),
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


def start_streaming_query(
    df: DataFrame,
    sink_type: str = "console",
    checkpoint_dir: str = "/tmp/airlytics-checkpoints",
    bigquery_table: Optional[str] = None,
    gcs_temp_bucket: Optional[str] = None,
    output_path: Optional[str] = None,
) -> StreamingQuery:
    """Start streaming writer for the processed DataFrame."""
    writer = df.writeStream.outputMode("complete" if "flight_leg_id" in df.columns else "append")

    if sink_type == "console":
        return (
            writer.format("console")
            .option("truncate", "false")
            .option("checkpointLocation", f"{checkpoint_dir}/console")
            .start()
        )
    elif sink_type == "bigquery" and bigquery_table:
        bq_writer = (
            writer.format("bigquery")
            .option("table", bigquery_table)
            .option("checkpointLocation", f"{checkpoint_dir}/bigquery")
        )
        if gcs_temp_bucket:
            bq_writer = bq_writer.option("temporaryGcsBucket", gcs_temp_bucket)
        return bq_writer.start()
    elif sink_type == "parquet" and output_path:
        return (
            writer.format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{checkpoint_dir}/parquet")
            .start()
        )
    else:
        raise ValueError(f"Unsupported sink_type: {sink_type}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Airlytics PySpark Structured Streaming Pipeline")
    parser.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "raw-flight-pings"))
    parser.add_argument("--sink", choices=["console", "bigquery", "parquet"], default="console")
    parser.add_argument("--checkpoint-dir", default=os.getenv("CHECKPOINT_DIR", "/tmp/airlytics-checkpoints"))
    parser.add_argument("--bq-table", default=os.getenv("BQ_TABLE", "airlytics-dev.curated_flights_dev.fact_flight_leg"))
    parser.add_argument("--gcs-temp-bucket", default=os.getenv("GCS_TEMP_BUCKET"))
    parser.add_argument("--parquet-path", default=os.getenv("PARQUET_PATH", "/tmp/airlytics-parquet"))
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID", "airlytics-dev"))
    args = parser.parse_args()

    if not PYSPARK_AVAILABLE:
        print("ERROR: pyspark is required. Install via: pip install -r spark/requirements.txt")
        sys.exit(1)

    spark = get_spark_session(gcp_project_id=args.project_id)
    logger.info("Spark session initialized. Subscribing to Kafka topic: %s @ %s", args.topic, args.kafka_bootstrap)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    watermarked_pings = parse_and_watermark_pings(raw_stream)
    flight_legs = sessionize_flight_legs(watermarked_pings)

    logger.info("Starting streaming sink: %s", args.sink)
    query = start_streaming_query(
        flight_legs,
        sink_type=args.sink,
        checkpoint_dir=args.checkpoint_dir,
        bigquery_table=args.bq_table,
        gcs_temp_bucket=args.gcs_temp_bucket,
        output_path=args.parquet_path,
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

