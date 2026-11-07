"""Apache Beam 5-minute fixed window aggregation transform for real-time airspace metrics."""

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Any, Dict, Iterable, Iterator, List, Tuple

try:
    import apache_beam as beam
    BEAM_AVAILABLE = True
except ImportError:
    beam = None  # type: ignore
    BEAM_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class AirspaceMetricsAccumulator:
    """Computes summary statistics for a 5-minute window across active aircraft."""

    window_start: str = ""
    window_end: str = ""
    total_pings: int = 0
    unique_aircraft: set = None  # type: ignore
    airborne_aircraft: set = None  # type: ignore
    on_ground_aircraft: set = None  # type: ignore
    altitude_sum: float = 0.0
    velocity_sum: float = 0.0

    def __post_init__(self):
        if self.unique_aircraft is None:
            self.unique_aircraft = set()
        if self.airborne_aircraft is None:
            self.airborne_aircraft = set()
        if self.on_ground_aircraft is None:
            self.on_ground_aircraft = set()

    def add_ping(self, ping: Dict[str, Any]) -> None:
        self.total_pings += 1
        icao = str(ping.get("icao24", "")).lower()
        on_ground = bool(ping.get("on_ground", False))
        alt = float(ping.get("baro_altitude") or 0.0)
        vel = float(ping.get("velocity") or 0.0)

        if icao:
            self.unique_aircraft.add(icao)
            if on_ground:
                self.on_ground_aircraft.add(icao)
            else:
                self.airborne_aircraft.add(icao)

        self.altitude_sum += alt
        self.velocity_sum += vel

    def to_dict(self) -> Dict[str, Any]:
        count = self.total_pings or 1
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "total_pings": self.total_pings,
            "active_aircraft_count": len(self.unique_aircraft),
            "airborne_aircraft_count": len(self.airborne_aircraft),
            "on_ground_aircraft_count": len(self.on_ground_aircraft),
            "avg_altitude_m": round(self.altitude_sum / count, 2),
            "avg_velocity_mps": round(self.velocity_sum / count, 2),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }


def aggregate_5m_window(
    pings: Iterable[Dict[str, Any]],
    window_start: str,
    window_end: str,
) -> Dict[str, Any]:
    """Pure Python aggregation of a 5-minute window for unit testing and local pipelines."""
    acc = AirspaceMetricsAccumulator(window_start=window_start, window_end=window_end)
    for p in pings:
        acc.add_ping(p)
    return acc.to_dict()


class Airspace5mAggregateDoFn(beam.DoFn if BEAM_AVAILABLE else object):  # type: ignore
    """Beam DoFn for computing 5-minute airspace window summaries."""

    def process(self, element: Tuple[Any, Iterable[Dict[str, Any]]], window=beam.DoFn.WindowParam if BEAM_AVAILABLE else None) -> Iterator[Dict[str, Any]]:  # type: ignore
        _, pings = element
        w_start = window.start.to_utc_datetime().isoformat() if window else ""
        w_end = window.end.to_utc_datetime().isoformat() if window else ""

        acc = AirspaceMetricsAccumulator(window_start=w_start, window_end=w_end)
        for p in pings:
            acc.add_ping(p)

        yield acc.to_dict()

