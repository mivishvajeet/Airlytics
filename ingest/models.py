"""Data models and normalization routines for OpenSky ADS-B flight state vectors."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

logger = logging.getLogger(__name__)


@dataclass
class NormalizedFlightPing:
    """Canonical schema for a normalized flight position ping."""

    message_id: str
    ingest_timestamp: str  # ISO 8601 UTC string
    event_timestamp: str   # ISO 8601 UTC string
    event_time_epoch: int  # Unix epoch seconds
    icao24: str
    callsign: str
    origin_country: str
    longitude: float
    latitude: float
    baro_altitude: Optional[float]
    on_ground: bool
    velocity: Optional[float]
    true_track: Optional[float]
    vertical_rate: Optional[float]
    geo_altitude: Optional[float]
    squawk: Optional[str]
    spi: bool
    position_source: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert dataclass instance to standard dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Serialize normalized ping to JSON string."""
        return json.dumps(self.to_dict())

    def get_pubsub_attributes(self) -> Dict[str, str]:
        """Generate filtering/routing attributes for Cloud Pub/Sub message."""
        return {
            "icao24": self.icao24,
            "event_timestamp": self.event_timestamp,
            "on_ground": "true" if self.on_ground else "false",
            "has_coordinates": "true",
        }


def parse_state_vector(
    vector: List[Any],
    response_time: int,
    ingest_dt: Optional[datetime] = None,
) -> Tuple[Optional[NormalizedFlightPing], Optional[Dict[str, Any]]]:
    """
    Parse a single raw OpenSky state vector array into a NormalizedFlightPing.

    OpenSky state vector index mapping:
      0: icao24 (string)
      1: callsign (string or None)
      2: origin_country (string)
      3: time_position (int or None)
      4: last_contact (int)
      5: longitude (float or None)
      6: latitude (float or None)
      7: baro_altitude (float or None)
      8: on_ground (boolean)
      9: velocity (float or None)
      10: true_track (float or None)
      11: vertical_rate (float or None)
      12: sensors (list or None)
      13: geo_altitude (float or None)
      14: squawk (string or None)
      15: spi (boolean)
      16: position_source (int)

    Returns:
        (NormalizedFlightPing, None) on success
        (None, error_dict) if record is unparseable or missing mandatory coordinates
    """
    if ingest_dt is None:
        ingest_dt = datetime.now(timezone.utc)
    ingest_iso = ingest_dt.isoformat()

    if not isinstance(vector, (list, tuple)) or len(vector) < 17:
        return None, {
            "reason": "invalid_vector_shape",
            "length": len(vector) if isinstance(vector, (list, tuple)) else 0,
            "raw": str(vector)[:200],
            "ingest_timestamp": ingest_iso,
        }

    raw_icao = vector[0]
    if not raw_icao or not isinstance(raw_icao, str):
        return None, {
            "reason": "missing_icao24",
            "raw": str(vector)[:200],
            "ingest_timestamp": ingest_iso,
        }
    icao24 = raw_icao.strip().lower()

    # Coordinates validation
    lon = vector[5]
    lat = vector[6]
    if lon is None or lat is None:
        return None, {
            "reason": "missing_coordinates",
            "icao24": icao24,
            "raw": str(vector)[:200],
            "ingest_timestamp": ingest_iso,
        }

    try:
        lon_f = float(lon)
        lat_f = float(lat)
        if not (-180.0 <= lon_f <= 180.0 and -90.0 <= lat_f <= 90.0):
            return None, {
                "reason": "coordinates_out_of_bounds",
                "icao24": icao24,
                "lon": lon,
                "lat": lat,
                "ingest_timestamp": ingest_iso,
            }
    except (ValueError, TypeError):
        return None, {
            "reason": "non_numeric_coordinates",
            "icao24": icao24,
            "lon": lon,
            "lat": lat,
            "ingest_timestamp": ingest_iso,
        }

    # Callsign normalization (strip whitespace)
    callsign_raw = vector[1]
    callsign = str(callsign_raw).strip() if callsign_raw is not None else ""

    origin_country = str(vector[2]) if vector[2] is not None else "Unknown"

    # Event timestamp resolution: prefer time_position, then last_contact, then response_time
    time_pos = vector[3]
    last_contact = vector[4]
    epoch_ts = time_pos if time_pos is not None else (last_contact if last_contact is not None else response_time)

    try:
        epoch_int = int(epoch_ts)
        event_dt = datetime.fromtimestamp(epoch_int, tz=timezone.utc)
        event_iso = event_dt.isoformat()
    except (ValueError, OSError, OverflowError):
        epoch_int = int(response_time)
        event_dt = datetime.fromtimestamp(epoch_int, tz=timezone.utc)
        event_iso = event_dt.isoformat()

    def safe_float(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    baro_alt = safe_float(vector[7])
    on_ground = bool(vector[8]) if vector[8] is not None else False
    velocity = safe_float(vector[9])
    true_track = safe_float(vector[10])
    vert_rate = safe_float(vector[11])
    geo_alt = safe_float(vector[13])
    squawk = str(vector[14]).strip() if vector[14] is not None else None
    spi = bool(vector[15]) if vector[15] is not None else False

    try:
        pos_source = int(vector[16]) if vector[16] is not None else 0
    except (ValueError, TypeError):
        pos_source = 0

    ping = NormalizedFlightPing(
        message_id=str(uuid.uuid4()),
        ingest_timestamp=ingest_iso,
        event_timestamp=event_iso,
        event_time_epoch=epoch_int,
        icao24=icao24,
        callsign=callsign,
        origin_country=origin_country,
        longitude=lon_f,
        latitude=lat_f,
        baro_altitude=baro_alt,
        on_ground=on_ground,
        velocity=velocity,
        true_track=true_track,
        vertical_rate=vert_rate,
        geo_altitude=geo_alt,
        squawk=squawk,
        spi=spi,
        position_source=pos_source,
    )

    return ping, None
