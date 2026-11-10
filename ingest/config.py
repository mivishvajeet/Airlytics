"""Configuration management for OpenSky Ingestion Service."""

from dataclasses import dataclass, field
import os
from typing import Optional


@dataclass(frozen=True)
class IngestionConfig:
    """Runtime configuration parsed from environment variables."""

    opensky_url: str = field(default_factory=lambda: os.getenv(
        "OPENSKY_URL", "https://opensky-network.org/api/states/all"
    ))
    opensky_username: Optional[str] = field(default_factory=lambda: os.getenv("OPENSKY_USERNAME") or None)
    opensky_password: Optional[str] = field(default_factory=lambda: os.getenv("OPENSKY_PASSWORD") or None)
    poll_interval_seconds: float = field(default_factory=lambda: float(os.getenv("POLL_INTERVAL_SECONDS", "10.0")))
    request_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("REQUEST_TIMEOUT_SECONDS", "15.0")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    backoff_factor: float = field(default_factory=lambda: float(os.getenv("BACKOFF_FACTOR", "2.0")))

    # Optional airspace bounding box; defaults to Western/Central Europe.
    bbox_lamin: Optional[float] = field(default_factory=lambda: float(os.getenv("BBOX_LAMIN")) if os.getenv("BBOX_LAMIN") else 45.0)
    bbox_lomin: Optional[float] = field(default_factory=lambda: float(os.getenv("BBOX_LOMIN")) if os.getenv("BBOX_LOMIN") else -5.0)
    bbox_lamax: Optional[float] = field(default_factory=lambda: float(os.getenv("BBOX_LAMAX")) if os.getenv("BBOX_LAMAX") else 55.0)
    bbox_lomax: Optional[float] = field(default_factory=lambda: float(os.getenv("BBOX_LOMAX")) if os.getenv("BBOX_LOMAX") else 15.0)

    streaming_sink: str = field(default_factory=lambda: os.getenv("STREAMING_SINK", "kafka").lower())
    kafka_bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "raw-flight-pings"))
    kafka_deadletter_topic: str = field(default_factory=lambda: os.getenv("KAFKA_DEADLETTER_TOPIC", "deadletter-pings"))
    kafka_snapshot_events_topic: str = field(default_factory=lambda: os.getenv("KAFKA_SNAPSHOT_EVENTS_TOPIC", "aircraft-observation-events"))

    # Persistent local state for snapshot comparison.
    snapshot_state_db: str = field(default_factory=lambda: os.getenv("SNAPSHOT_STATE_DB", "data/state/aircraft_state.sqlite"))
    snapshot_missing_threshold: int = field(default_factory=lambda: int(os.getenv("SNAPSHOT_MISSING_THRESHOLD", "3")))

    # Legacy cloud settings retained for historical reference only.
    gcp_project_id: str = field(default_factory=lambda: os.getenv("GCP_PROJECT_ID", "airlytics-dev-123456"))
    pubsub_topic_id: str = field(default_factory=lambda: os.getenv("PUBSUB_TOPIC_ID", "raw-flight-pings"))
    deadletter_topic_id: str = field(default_factory=lambda: os.getenv("DEADLETTER_TOPIC_ID", "deadletter-pings"))
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "500")))
    batch_timeout_seconds: float = field(default_factory=lambda: float(os.getenv("BATCH_TIMEOUT_SECONDS", "0.5")))
    run_duration_seconds: int = field(default_factory=lambda: int(os.getenv("RUN_DURATION_SECONDS", "0")))

    @property
    def has_bbox(self) -> bool:
        return all(v is not None for v in (
            self.bbox_lamin, self.bbox_lomin, self.bbox_lamax, self.bbox_lomax
        ))

    @property
    def is_authenticated(self) -> bool:
        return bool(self.opensky_username and self.opensky_password)
