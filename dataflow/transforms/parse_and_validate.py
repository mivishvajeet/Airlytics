"""Apache Beam transform for parsing Pub/Sub messages, schema validation, and DLQ routing."""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Iterator, Optional, Tuple
import uuid

try:
    import apache_beam as beam
    from apache_beam import pvalue
    BEAM_AVAILABLE = True
except ImportError:
    beam = None  # type: ignore
    pvalue = None  # type: ignore
    BEAM_AVAILABLE = False

logger = logging.getLogger(__name__)

DEADLETTER_TAG = "deadletter"


def parse_and_validate_payload(raw_bytes: bytes) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Parse raw message payload into a validated flight ping dictionary.

    Returns:
        (valid_dict, None) on success
        (None, deadletter_dict) on deserialization or schema error
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        text = raw_bytes.decode("utf-8")
        data = json.loads(text)
    except Exception as err:
        return None, {
            "deadletter_id": str(uuid.uuid4()),
            "received_timestamp": now_iso,
            "reason": f"json_decode_error: {str(err)}",
            "raw_payload": str(raw_bytes)[:1000],
            "source_topic": "raw-flight-pings",
        }

    if not isinstance(data, dict):
        return None, {
            "deadletter_id": str(uuid.uuid4()),
            "received_timestamp": now_iso,
            "reason": "payload_not_a_json_object",
            "raw_payload": str(text)[:1000],
            "source_topic": "raw-flight-pings",
        }

    # Mandatory field checks
    icao24 = data.get("icao24")
    if not icao24 or not isinstance(icao24, str):
        return None, {
            "deadletter_id": str(uuid.uuid4()),
            "received_timestamp": now_iso,
            "reason": "missing_or_invalid_icao24",
            "raw_payload": str(text)[:1000],
            "source_topic": "raw-flight-pings",
        }

    lat = data.get("latitude")
    lon = data.get("longitude")
    if lat is None or lon is None:
        return None, {
            "deadletter_id": str(uuid.uuid4()),
            "received_timestamp": now_iso,
            "reason": "missing_coordinates",
            "raw_payload": str(text)[:1000],
            "source_topic": "raw-flight-pings",
        }

    try:
        lat_f = float(lat)
        lon_f = float(lon)
        if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
            return None, {
                "deadletter_id": str(uuid.uuid4()),
                "received_timestamp": now_iso,
                "reason": "coordinates_out_of_bounds",
                "raw_payload": str(text)[:1000],
                "source_topic": "raw-flight-pings",
            }
    except (ValueError, TypeError):
        return None, {
            "deadletter_id": str(uuid.uuid4()),
            "received_timestamp": now_iso,
            "reason": "non_numeric_coordinates",
            "raw_payload": str(text)[:1000],
            "source_topic": "raw-flight-pings",
        }

    # Ensure required timestamps and IDs exist
    if not data.get("message_id"):
        data["message_id"] = str(uuid.uuid4())
    if not data.get("ingest_timestamp"):
        data["ingest_timestamp"] = now_iso
    if not data.get("event_timestamp"):
        epoch = data.get("event_time_epoch", int(datetime.now(timezone.utc).timestamp()))
        data["event_timestamp"] = datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
    if "event_time_epoch" not in data:
        data["event_time_epoch"] = int(datetime.now(timezone.utc).timestamp())

    data["icao24"] = icao24.strip().lower()
    data["callsign"] = str(data.get("callsign", "")).strip()
    data["latitude"] = lat_f
    data["longitude"] = lon_f
    data["on_ground"] = bool(data.get("on_ground", False))

    return data, None


class ParseAndValidatePingFn(beam.DoFn if BEAM_AVAILABLE else object):  # type: ignore
    """Beam DoFn for message parsing with dead-letter side output."""

    def process(self, element: Any) -> Iterator[Any]:
        """
        Process Pub/Sub message (bytes or object).
        Emits valid ping or TaggedOutput(DEADLETTER_TAG, record).
        """
        if isinstance(element, bytes):
            raw_bytes = element
        elif hasattr(element, "data"):
            raw_bytes = element.data
        elif isinstance(element, str):
            raw_bytes = element.encode("utf-8")
        elif isinstance(element, dict):
            raw_bytes = json.dumps(element).encode("utf-8")
        else:
            raw_bytes = str(element).encode("utf-8")

        valid_ping, dlq_record = parse_and_validate_payload(raw_bytes)

        if valid_ping is not None:
            yield valid_ping
        elif dlq_record is not None:
            if BEAM_AVAILABLE and pvalue is not None:
                yield pvalue.TaggedOutput(DEADLETTER_TAG, dlq_record)
            else:
                yield ("deadletter", dlq_record)
