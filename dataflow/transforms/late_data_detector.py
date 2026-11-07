"""Apache Beam transform for identifying late data and routing to late side output."""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, Iterator, Optional, Tuple

try:
    import apache_beam as beam
    from apache_beam import pvalue
    BEAM_AVAILABLE = True
except ImportError:
    beam = None  # type: ignore
    pvalue = None  # type: ignore
    BEAM_AVAILABLE = False

logger = logging.getLogger(__name__)

LATE_TAG = "late_pings"


def evaluate_lateness(
    ping: Dict[str, Any],
    allowed_lateness_seconds: float = 600.0,  # 10 minutes
    reference_epoch: Optional[float] = None,
) -> Tuple[bool, float]:
    """
    Determine if a flight ping is late relative to a reference timestamp (watermark / clock).

    Returns:
        (is_late, lateness_seconds)
    """
    if reference_epoch is None:
        reference_epoch = datetime.now(timezone.utc).timestamp()

    event_epoch = float(ping.get("event_time_epoch", reference_epoch))
    lateness = reference_epoch - event_epoch

    is_late = lateness > allowed_lateness_seconds
    return is_late, max(0.0, lateness)


class LateDataFilterFn(beam.DoFn if BEAM_AVAILABLE else object):  # type: ignore
    """
    DoFn that flags events that exceed maximum allowed lateness.
    Emits on-time events to main output, and late events to TaggedOutput(LATE_TAG).
    """

    def __init__(self, allowed_lateness_seconds: float = 600.0):
        self.allowed_lateness_seconds = allowed_lateness_seconds

    def process(self, element: Dict[str, Any]) -> Iterator[Any]:
        is_late, lateness_sec = evaluate_lateness(
            element, allowed_lateness_seconds=self.allowed_lateness_seconds
        )

        if is_late:
            late_record = dict(element)
            late_record["lateness_seconds"] = lateness_sec
            if BEAM_AVAILABLE and pvalue is not None:
                yield pvalue.TaggedOutput(LATE_TAG, late_record)
            else:
                yield (LATE_TAG, late_record)
        else:
            yield element
