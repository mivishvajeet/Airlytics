"""Apache Beam sessionization transform for reconstructing discrete flight legs."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import logging
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

try:
    import apache_beam as beam
    BEAM_AVAILABLE = True
except ImportError:
    beam = None  # type: ignore
    BEAM_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class FlightLegAccumulator:
    """Accumulates and summarizes telemetry pings within a single session window."""

    icao24: str
    callsign: str = ""
    start_time_epoch: int = 0
    end_time_epoch: int = 0
    origin_lat: Optional[float] = None
    origin_lon: Optional[float] = None
    dest_lat: Optional[float] = None
    dest_lon: Optional[float] = None
    max_altitude_m: float = 0.0
    max_velocity_mps: float = 0.0
    velocity_sum: float = 0.0
    velocity_count: int = 0
    total_pings: int = 0
    start_iso: str = ""
    end_iso: str = ""

    def add_ping(self, ping: Dict[str, Any]) -> None:
        """Incorporate a single telemetry ping into the session."""
        self.total_pings += 1

        # Callsign priority
        callsign = str(ping.get("callsign", "")).strip()
        if callsign and not self.callsign:
            self.callsign = callsign

        epoch = int(ping.get("event_time_epoch", 0))
        lat = float(ping.get("latitude", 0.0))
        lon = float(ping.get("longitude", 0.0))
        alt = float(ping.get("baro_altitude") or ping.get("geo_altitude") or 0.0)
        vel = float(ping.get("velocity") or 0.0)

        if self.start_time_epoch == 0 or epoch < self.start_time_epoch:
            self.start_time_epoch = epoch
            self.origin_lat = lat
            self.origin_lon = lon
            self.start_iso = ping.get("event_timestamp") or datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

        if epoch >= self.end_time_epoch:
            self.end_time_epoch = epoch
            self.dest_lat = lat
            self.dest_lon = lon
            self.end_iso = ping.get("event_timestamp") or datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

        if alt > self.max_altitude_m:
            self.max_altitude_m = alt

        if vel > self.max_velocity_mps:
            self.max_velocity_mps = vel

        if vel > 0:
            self.velocity_sum += vel
            self.velocity_count += 1

    def to_curated_record(self, min_pings: int = 2) -> Optional[Dict[str, Any]]:
        """
        Produce a curated fact_flight_leg dictionary.
        Returns None if session does not meet minimum quality thresholds.
        """
        if self.total_pings < min_pings:
            return None

        duration_sec = max(0, self.end_time_epoch - self.start_time_epoch)
        duration_min = round(duration_sec / 60.0, 2)

        # Deterministic unique surrogate key
        hash_src = f"{self.icao24}_{self.start_iso}"
        flight_leg_id = hashlib.md5(hash_src.encode("utf-8")).hexdigest()

        avg_vel = round(self.velocity_sum / self.velocity_count, 2) if self.velocity_count > 0 else 0.0

        return {
            "flight_leg_id": flight_leg_id,
            "icao24": self.icao24,
            "callsign": self.callsign,
            "start_timestamp": self.start_iso,
            "end_timestamp": self.end_iso,
            "duration_minutes": duration_min,
            "origin_lat": self.origin_lat,
            "origin_lon": self.origin_lon,
            "dest_lat": self.dest_lat,
            "dest_lon": self.dest_lon,
            "max_altitude_m": round(self.max_altitude_m, 2),
            "max_velocity_mps": round(self.max_velocity_mps, 2),
            "avg_velocity_mps": avg_vel,
            "total_pings": self.total_pings,
            "session_gap_minutes": 15.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }


def sessionize_pings(
    pings: Iterable[Dict[str, Any]],
    gap_seconds: int = 900,  # 15 minutes
    min_pings: int = 2,
) -> List[Dict[str, Any]]:
    """
    Sessionize a chronological stream of pings for a single aircraft key.
    Splits when signal gap between successive pings exceeds gap_seconds.
    """
    sorted_pings = sorted(pings, key=lambda p: int(p.get("event_time_epoch", 0)))
    sessions: List[Dict[str, Any]] = []

    current_acc: Optional[FlightLegAccumulator] = None
    last_epoch = 0

    for ping in sorted_pings:
        epoch = int(ping.get("event_time_epoch", 0))
        icao24 = str(ping.get("icao24", "")).lower()

        if current_acc is None:
            current_acc = FlightLegAccumulator(icao24=icao24)
            current_acc.add_ping(ping)
            last_epoch = epoch
            continue

        if (epoch - last_epoch) > gap_seconds:
            # Emit completed session and start a new one
            rec = current_acc.to_curated_record(min_pings=min_pings)
            if rec is not None:
                sessions.append(rec)
            current_acc = FlightLegAccumulator(icao24=icao24)
            current_acc.add_ping(ping)
        else:
            current_acc.add_ping(ping)

        last_epoch = epoch

    if current_acc is not None:
        rec = current_acc.to_curated_record(min_pings=min_pings)
        if rec is not None:
            sessions.append(rec)

    return sessions


class SessionizeFlightLegsDoFn(beam.DoFn if BEAM_AVAILABLE else object):  # type: ignore
    """
    Beam DoFn that executes inside an Apache Beam Sessions window.
    Takes (icao24, Iterable[ping]) and outputs a single aggregated flight leg record.
    """

    def __init__(self, min_pings: int = 2):
        self.min_pings = min_pings

    def process(self, element: Tuple[str, Iterable[Dict[str, Any]]]) -> Iterator[Dict[str, Any]]:
        icao24, pings = element
        accumulator = FlightLegAccumulator(icao24=icao24)

        for ping in pings:
            accumulator.add_ping(ping)

        curated = accumulator.to_curated_record(min_pings=self.min_pings)
        if curated is not None:
            yield curated
