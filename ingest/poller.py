"""OpenSky REST API client with exponential backoff and rate-limit resilience."""

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import random
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request

try:
    import requests
    from requests.auth import HTTPBasicAuth
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None  # type: ignore
    HTTPBasicAuth = None  # type: ignore
    REQUESTS_AVAILABLE = False

from ingest.config import IngestionConfig
from ingest.models import NormalizedFlightPing, parse_state_vector

logger = logging.getLogger(__name__)


@dataclass
class PollResult:
    """Outcome of a single polling iteration."""

    response_time: int
    total_states_received: int
    observed_icao24s: List[str]
    valid_pings: List[NormalizedFlightPing]
    deadletter_records: List[Dict[str, Any]]
    api_latency_ms: float
    status_code: int


class OpenSkyPoller:
    """Client for polling live ADS-B state vectors from OpenSky."""

    def __init__(self, config: IngestionConfig, session: Optional[Any] = None):
        self.config = config
        self.session = session or (requests.Session() if REQUESTS_AVAILABLE else None)
        self._consecutive_rate_limits = 0

    def _build_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if self.config.has_bbox:
            params.update({
                "lamin": self.config.bbox_lamin,
                "lomin": self.config.bbox_lomin,
                "lamax": self.config.bbox_lamax,
                "lomax": self.config.bbox_lomax,
            })
        return params

    def _execute_http_get(self, url: str, params: Dict[str, Any], headers: Dict[str, str]) -> Tuple[int, Dict[str, Any]]:
        if self.session is not None and hasattr(self.session, "get"):
            auth = None
            if self.config.is_authenticated:
                auth = HTTPBasicAuth(self.config.opensky_username, self.config.opensky_password) if REQUESTS_AVAILABLE and HTTPBasicAuth else (self.config.opensky_username, self.config.opensky_password)
            resp = self.session.get(url, params=params, auth=auth, headers=headers, timeout=self.config.request_timeout_seconds)
            data = resp.json() if resp.status_code == 200 else {}
            return resp.status_code, data

        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}" if query_string else url
        req = urllib.request.Request(full_url, headers=headers, method="GET")
        if self.config.is_authenticated:
            user_pass = f"{self.config.opensky_username}:{self.config.opensky_password or ''}"
            encoded_auth = base64.b64encode(user_pass.encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {encoded_auth}")

        try:
            with urllib.request.urlopen(req, timeout=self.config.request_timeout_seconds) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as http_err:
            return http_err.code, {}

    def fetch_states(self) -> Tuple[int, Dict[str, Any], float]:
        params = self._build_params()
        headers = {"User-Agent": "Airlytics-Data-Platform/1.0 (Personal/Portfolio Data Engineering Project)"}

        for attempt in range(1, self.config.max_retries + 1):
            t_start = time.perf_counter()
            try:
                status_code, payload = self._execute_http_get(self.config.opensky_url, params, headers)
                latency_ms = (time.perf_counter() - t_start) * 1000.0

                if status_code == 200:
                    self._consecutive_rate_limits = 0
                    return 200, payload, latency_ms

                if status_code == 429:
                    self._consecutive_rate_limits += 1
                    sleep_time = (self.config.backoff_factor ** attempt) * 2.0 + random.uniform(0.5, 2.0)
                    logger.warning("OpenSky rate limit encountered (HTTP 429, attempt %d/%d). Cooling down for %.2fs...", attempt, self.config.max_retries, sleep_time)
                    time.sleep(sleep_time)
                    continue

                if 500 <= status_code < 600:
                    sleep_time = (self.config.backoff_factor ** attempt) + random.uniform(0.1, 0.5)
                    logger.warning("OpenSky server error (HTTP %d, attempt %d/%d). Retrying in %.2fs...", status_code, attempt, self.config.max_retries, sleep_time)
                    time.sleep(sleep_time)
                    continue

                logger.error("Unexpected OpenSky HTTP error %d", status_code)
                return status_code, {}, latency_ms

            except Exception as err:
                latency_ms = (time.perf_counter() - t_start) * 1000.0
                sleep_time = (self.config.backoff_factor ** attempt) + random.uniform(0.1, 0.5)
                logger.warning("Network error querying OpenSky (%s, attempt %d/%d). Retrying in %.2fs...", err, attempt, self.config.max_retries, sleep_time)
                time.sleep(sleep_time)

        logger.error("Exceeded maximum retries (%d) polling OpenSky API.", self.config.max_retries)
        return 503, {}, 0.0

    def poll_cycle(self) -> PollResult:
        """Fetch one snapshot; retain raw ICAO24s separately from valid position pings."""
        status_code, payload, latency_ms = self.fetch_states()
        now_dt = datetime.now(timezone.utc)

        if status_code != 200:
            return PollResult(
                response_time=int(now_dt.timestamp()),
                total_states_received=0,
                observed_icao24s=[],
                valid_pings=[],
                deadletter_records=[{
                    "reason": "http_fetch_failure",
                    "status_code": status_code,
                    "timestamp": now_dt.isoformat(),
                }],
                api_latency_ms=latency_ms,
                status_code=status_code,
            )

        # A successful empty response is a valid empty snapshot. Never confuse
        # it with an HTTP/network failure.
        resp_time = payload.get("time", int(now_dt.timestamp()))
        states = payload.get("states") or []
        observed_icao24s = [
            str(vector[0]).strip().lower()
            for vector in states
            if isinstance(vector, (list, tuple))
            and len(vector) > 0
            and isinstance(vector[0], str)
            and vector[0].strip()
        ]

        valid_pings: List[NormalizedFlightPing] = []
        deadletter_records: List[Dict[str, Any]] = []
        for vector in states:
            ping, err = parse_state_vector(vector, response_time=resp_time, ingest_dt=now_dt)
            if ping is not None:
                valid_pings.append(ping)
            elif err is not None:
                deadletter_records.append(err)

        return PollResult(
            response_time=resp_time,
            total_states_received=len(states),
            observed_icao24s=observed_icao24s,
            valid_pings=valid_pings,
            deadletter_records=deadletter_records,
            api_latency_ms=latency_ms,
            status_code=status_code,
        )
