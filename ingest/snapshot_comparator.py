"""Persistent comparison of consecutive OpenSky aircraft snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Any, Dict, Iterable, List
import uuid

logger = logging.getLogger(__name__)


@dataclass
class SnapshotComparison:
    """Summary of one successfully processed OpenSky snapshot."""

    baseline: bool
    observed_count: int
    newly_observed_count: int
    no_longer_observed_count: int
    reappeared_count: int
    events: List[Dict[str, Any]]


class SnapshotComparator:
    """Compare aircraft identifiers and persist state across process restarts.

    An aircraft is marked no longer observed only after it is absent from
    missing_threshold consecutive successful snapshots. This is an
    observation-status inference, not proof that the aircraft left an area.
    """

    def __init__(self, database_path: str, missing_threshold: int = 3):
        if missing_threshold < 1:
            raise ValueError("missing_threshold must be at least 1")

        self.database_path = database_path
        self.missing_threshold = missing_threshold

        if database_path != ":memory:":
            Path(database_path).expanduser().parent.mkdir(parents=True, exist_ok=True)

        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize_database(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS aircraft_observation_state (
                    icao24 TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    consecutive_misses INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS comparator_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    @staticmethod
    def _event(
        event_type: str,
        icao24: str,
        event_timestamp: str,
        last_seen_at: str,
        consecutive_misses: int = 0,
    ) -> Dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "icao24": icao24,
            "event_timestamp": event_timestamp,
            "last_seen_at": last_seen_at,
            "consecutive_misses": consecutive_misses,
            "source": "opensky",
        }

    def process_snapshot(
        self,
        icao24s: Iterable[str],
        response_time: int,
    ) -> SnapshotComparison:
        """Process one successful snapshot of observed ICAO24 identifiers."""
        observed_ids = {
            str(value).strip().lower()
            for value in icao24s
            if value is not None and str(value).strip()
        }
        snapshot_at = datetime.fromtimestamp(
            int(response_time), tz=timezone.utc
        ).isoformat()

        events: List[Dict[str, Any]] = []
        newly_observed = 0
        no_longer_observed = 0
        reappeared = 0

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            initialized = connection.execute(
                "SELECT value FROM comparator_metadata WHERE key = 'initialized'"
            ).fetchone()

            # The first successful snapshot establishes a baseline. It should
            # not generate a burst of false newly-observed events.
            if initialized is None:
                for icao24 in observed_ids:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO aircraft_observation_state
                        (icao24, first_seen_at, last_seen_at, is_active, consecutive_misses)
                        VALUES (?, ?, ?, 1, 0)
                        """,
                        (icao24, snapshot_at, snapshot_at),
                    )
                connection.execute(
                    "INSERT INTO comparator_metadata (key, value) VALUES ('initialized', 'true')"
                )
                return SnapshotComparison(
                    baseline=True,
                    observed_count=len(observed_ids),
                    newly_observed_count=0,
                    no_longer_observed_count=0,
                    reappeared_count=0,
                    events=[],
                )

            rows = connection.execute(
                "SELECT * FROM aircraft_observation_state"
            ).fetchall()
            existing = {row["icao24"]: row for row in rows}

            for icao24 in observed_ids:
                row = existing.get(icao24)
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO aircraft_observation_state
                        (icao24, first_seen_at, last_seen_at, is_active, consecutive_misses)
                        VALUES (?, ?, ?, 1, 0)
                        """,
                        (icao24, snapshot_at, snapshot_at),
                    )
                    events.append(
                        self._event("newly_observed", icao24, snapshot_at, snapshot_at)
                    )
                    newly_observed += 1
                elif not row["is_active"]:
                    connection.execute(
                        """
                        UPDATE aircraft_observation_state
                        SET last_seen_at = ?, is_active = 1, consecutive_misses = 0
                        WHERE icao24 = ?
                        """,
                        (snapshot_at, icao24),
                    )
                    events.append(
                        self._event(
                            "reappeared",
                            icao24,
                            snapshot_at,
                            row["last_seen_at"],
                        )
                    )
                    reappeared += 1
                else:
                    connection.execute(
                        """
                        UPDATE aircraft_observation_state
                        SET last_seen_at = ?, consecutive_misses = 0
                        WHERE icao24 = ?
                        """,
                        (snapshot_at, icao24),
                    )

            for icao24, row in existing.items():
                if icao24 in observed_ids or not row["is_active"]:
                    continue

                misses = int(row["consecutive_misses"]) + 1
                if misses >= self.missing_threshold:
                    connection.execute(
                        """
                        UPDATE aircraft_observation_state
                        SET is_active = 0, consecutive_misses = ?
                        WHERE icao24 = ?
                        """,
                        (misses, icao24),
                    )
                    events.append(
                        self._event(
                            "no_longer_observed",
                            icao24,
                            snapshot_at,
                            row["last_seen_at"],
                            misses,
                        )
                    )
                    no_longer_observed += 1
                else:
                    connection.execute(
                        """
                        UPDATE aircraft_observation_state
                        SET consecutive_misses = ?
                        WHERE icao24 = ?
                        """,
                        (misses, icao24),
                    )

        return SnapshotComparison(
            baseline=False,
            observed_count=len(observed_ids),
            newly_observed_count=newly_observed,
            no_longer_observed_count=no_longer_observed,
            reappeared_count=reappeared,
            events=events,
        )
