"""Production Dataflow streaming pipeline for real-time flight tracking analytics."""

import argparse
from datetime import datetime, timezone
import logging
import sys
from typing import Any, Dict

try:
    import apache_beam as beam
    from apache_beam.options.pipeline_options import (
        GoogleCloudOptions,
        PipelineOptions,
        SetupOptions,
        StandardOptions,
        WorkerOptions,
    )
    from apache_beam.transforms import window
    from apache_beam.transforms.trigger import (
        AccumulationMode,
        AfterProcessingTime,
        AfterWatermark,
        Repeatedly,
    )
    BEAM_AVAILABLE = True
except ImportError:
    beam = None  # type: ignore
    BEAM_AVAILABLE = False

from dataflow.schemas import (
    DEADLETTER_PINGS_SCHEMA,
    FLIGHT_LEGS_SCHEMA,
    LATE_PINGS_SCHEMA,
    RAW_PINGS_SCHEMA,
)
from dataflow.transforms.fixed_window_aggregates import Airspace5mAggregateDoFn
from dataflow.transforms.late_data_detector import LATE_TAG, LateDataFilterFn
from dataflow.transforms.parse_and_validate import DEADLETTER_TAG, ParseAndValidatePingFn
from dataflow.transforms.sessionize_flight_legs import SessionizeFlightLegsDoFn

logger = logging.getLogger("airlytics-pipeline")


class AirlyticsPipelineOptions(PipelineOptions if BEAM_AVAILABLE else object):  # type: ignore
    """CLI flags and runtime parameters for Airlytics Dataflow Flex Template."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_subscription",
            type=str,
            help="Pub/Sub subscription to consume flight pings from",
        )
        parser.add_argument(
            "--input_topic",
            type=str,
            help="Pub/Sub topic to consume flight pings from (alternative to subscription)",
        )
        parser.add_argument(
            "--raw_table",
            type=str,
            default="airlytics-dev-123456:raw_flights.raw_pings",
            help="BigQuery table spec for raw flight pings sink",
        )
        parser.add_argument(
            "--late_table",
            type=str,
            default="airlytics-dev-123456:raw_flights.late_pings",
            help="BigQuery table spec for late flight pings sink",
        )
        parser.add_argument(
            "--deadletter_table",
            type=str,
            default="airlytics-dev-123456:raw_flights.deadletter_pings",
            help="BigQuery table spec for dead-letter errors sink",
        )
        parser.add_argument(
            "--flight_legs_table",
            type=str,
            default="airlytics-dev-123456:curated_flights.fact_flight_leg",
            help="BigQuery table spec for sessionized flight legs sink",
        )
        parser.add_argument(
            "--session_gap_minutes",
            type=float,
            default=15.0,
            help="Gap duration in minutes to delineate discrete flight legs",
        )
        parser.add_argument(
            "--allowed_lateness_minutes",
            type=float,
            default=10.0,
            help="Allowed lateness in minutes for streaming windows",
        )


def build_pipeline(pipeline, custom_options):
    """Construct the DAG for streaming ingestion, sessionization, and BigQuery sinks."""
    if not BEAM_AVAILABLE:
        raise RuntimeError("apache-beam must be installed to build and execute the pipeline.")

    # 1. Read from Pub/Sub
    if custom_options.input_subscription:
        raw_messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=custom_options.input_subscription,
            with_attributes=False,
        )
    elif custom_options.input_topic:
        raw_messages = pipeline | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            topic=custom_options.input_topic,
            with_attributes=False,
        )
    else:
        raise ValueError("Either --input_subscription or --input_topic must be provided.")

    # 2. Parse and Validate Schema with Dead-Letter Side Output
    parsed_results = raw_messages | "ParseAndValidate" >> beam.ParDo(
        ParseAndValidatePingFn()
    ).with_outputs(DEADLETTER_TAG, main="valid_pings")

    valid_pings = parsed_results["valid_pings"]
    deadletter_pings = parsed_results[DEADLETTER_TAG]

    # 3. Dead-Letter Sink to BigQuery
    _ = (
        deadletter_pings
        | "WriteDeadLetterToBQ"
        >> beam.io.WriteToBigQuery(
            table=custom_options.deadletter_table,
            schema=DEADLETTER_PINGS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

    # 4. Raw Storage Sink: Direct append of all valid pings
    _ = (
        valid_pings
        | "WriteRawPingsToBQ"
        >> beam.io.WriteToBigQuery(
            table=custom_options.raw_table,
            schema=RAW_PINGS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method="STREAMING_INSERTS",
        )
    )

    # 5. Check Lateness and Separate Late Pings
    allowed_lateness_sec = float(custom_options.allowed_lateness_minutes) * 60.0
    lateness_results = valid_pings | "CheckLateness" >> beam.ParDo(
        LateDataFilterFn(allowed_lateness_seconds=allowed_lateness_sec)
    ).with_outputs(LATE_TAG, main="on_time_pings")

    on_time_pings = lateness_results["on_time_pings"]
    late_pings = lateness_results[LATE_TAG]

    # Write late pings side output to BigQuery
    _ = (
        late_pings
        | "WriteLatePingsToBQ"
        >> beam.io.WriteToBigQuery(
            table=custom_options.late_table,
            schema=LATE_PINGS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

    # 6. Assign Event Timestamps from OpenSky Telemetry
    timestamped_pings = on_time_pings | "AssignTimestamps" >> beam.Map(
        lambda p: beam.window.TimestampedValue(p, p["event_time_epoch"])
    )

    # 7. Sessionization: Group by icao24 into Session Windows (15-min gap)
    session_gap_sec = float(custom_options.session_gap_minutes) * 60.0
    session_windows = (
        timestamped_pings
        | "KeyByIcao24" >> beam.Map(lambda p: (p["icao24"], p))
        | "SessionWindowing"
        >> beam.WindowInto(
            window.Sessions(gap_size=session_gap_sec),
            trigger=Repeatedly(AfterWatermark(early=AfterProcessingTime(60))),
            accumulation_mode=AccumulationMode.DISCARDING,
            allowed_lateness=allowed_lateness_sec,
        )
        | "GroupByAircraftSession" >> beam.GroupByKey()
        | "SessionizeFlightLegs" >> beam.ParDo(SessionizeFlightLegsDoFn(min_pings=2))
    )

    # 8. Write Sessionized Legs Candidates to BigQuery
    _ = (
        session_windows
        | "WriteFlightLegsToBQ"
        >> beam.io.WriteToBigQuery(
            table=custom_options.flight_legs_table,
            schema=FLIGHT_LEGS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        )
    )

    return pipeline


def run(argv=None):
    """Pipeline entrypoint."""
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(AirlyticsPipelineOptions)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Enforce cost controls: personal project capped at 2 workers
    worker_options = pipeline_options.view_as(WorkerOptions)
    if worker_options.max_num_workers is None or worker_options.max_num_workers > 2:
        worker_options.max_num_workers = 2
        logger.info("Enforcing cost constraint: max_num_workers capped at 2")

    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(p, custom_options)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run(sys.argv)
