# 03 — Data engineering and analytics concepts

## 1. From API response to analytical dataset

Airlytics starts with repeated snapshots of aircraft state. Each row is an observation at a point in time, not a complete flight record. The data engineering task is to turn those observations into consistent events and then into datasets that answer analytical questions.

Conceptual flow:

```text
OpenSky state vectors
  -> raw API response
  -> parsed / normalized ping
  -> Kafka event
  -> Spark streaming transformation
  -> Parquet session output
  -> DuckDB SQL analysis
  -> dbt staging and marts
  -> Streamlit dashboard
```

The final DuckDB/dbt/dashboard connection is a target architecture, not a currently verified end-to-end path. The SQL models are still BigQuery-specific and the dashboard provider has known integration mismatches.

## 2. Raw, staging, and mart layers

A useful medallion-style vocabulary:

| Layer | Meaning | Airlytics interpretation |
|---|---|---|
| Raw / Bronze | Preserve source-shaped data with minimal changes | Original API payload or normalized event landing data |
| Clean / Silver | Validated, typed, deduplicated, standardized records | Canonical flight pings with usable coordinates and timestamps |
| Curated / Gold | Business/analytical entities and aggregates | Aircraft sessions, route summaries, airport activity metrics |

Do not claim that these three physical layers are fully implemented unless the repository has separate, populated datasets for them. At present, the Spark output path is named `data/bronze/flight-legs`, even though the output represents derived session aggregates. That naming should be reconciled: raw pings belong in a raw/bronze location; derived sessions are closer to curated/silver or gold depending on the project contract.

### Example transformation

Raw API arrays are positional and can contain nulls. A normalized record gives fields stable names:

| Field | Purpose |
|---|---|
| `icao24` | Aircraft transponder identifier |
| `callsign` | Broadcast callsign; may be null/blank |
| `origin_country` | Country field supplied by the source |
| `longitude`, `latitude` | Position coordinates |
| `baro_altitude` | Barometric altitude in metres |
| `velocity` | Speed in metres per second |
| `true_track` | Track/heading in degrees |
| `event_timestamp` | Time associated with the observed state |
| `ingest_timestamp` | Time the application ingested/normalized it |

Preserve source units in storage or make unit conversions explicit in column names. For example, metres per second should not be labelled knots. A dashboard can convert speed to knots for display using approximately 1.94384 knots per m/s.

## 3. Event time versus processing time

- **Event time**: when the aircraft state was observed according to source data.
- **Ingestion time**: when the poller received/normalized the response.
- **Processing time**: when Spark processed the record.

These clocks answer different questions. Event time is used for chronological analytics; ingestion minus event time can approximate source-to-ingestion delay; processing minus ingestion can help measure pipeline lag if the ingestion timestamp is carried through.

A useful freshness metric is:

`freshness_seconds = ingestion_timestamp - event_timestamp`

Check clock skew, missing source timestamps, and timezone handling before trusting this metric.

## 4. Streaming, micro-batches and watermarks

Spark Structured Streaming presents a streaming DataFrame API and processes data incrementally. Depending on the source and trigger configuration, execution commonly occurs in micro-batches.

A **watermark** tells Spark how late event-time data may arrive while still participating in stateful operations. It helps limit retained aggregation state. A 10-minute watermark does not mean the pipeline waits exactly ten minutes for every record, and it does not guarantee that every later record is included.

A **session window** groups events for one key when the gap between consecutive events is below a configured inactivity threshold. With a 15-minute gap, an aircraft's observations can form one session until there is a sufficiently long gap. This is an analytical heuristic. It cannot establish an actual flight leg, takeoff/landing, origin or destination by itself.

Test late events around the threshold:
- event arrives within the allowed lateness window;
- event arrives after the watermark has advanced;
- events have equal timestamps;
- aircraft stops transmitting and resumes;
- application restarts from checkpoint.

## 5. Kafka concepts in this project

- **Broker**: Kafka server accepting and storing records.
- **Topic**: named stream, here `raw-flight-pings` and `deadletter-pings`.
- **Partition**: ordered append-only log segment within a topic.
- **Key**: `icao24`; records with the same key are routed consistently to a partition under the partitioner.
- **Offset**: position of a record within a partition.
- **Consumer**: Spark reads records from Kafka.
- **Consumer group/checkpoint state**: tracks progress for the streaming query.

Ordering is only guaranteed within a partition, not globally across all aircraft. A single-broker local cluster is suitable for learning, not high availability.

## 6. Data quality rules worth enforcing

| Rule | Why it matters | Suggested action |
|---|---|---|
| `icao24` present and normalized | Stable aircraft key | Reject or quarantine missing/invalid IDs |
| Latitude in [-90, 90] | Geographic validity | Reject out-of-range values |
| Longitude in [-180, 180] | Geographic validity | Reject out-of-range values |
| Coordinates not null | Position analytics require location | Keep non-position observations separately if useful |
| Timestamp parseable | Event-time calculations | DLQ malformed timestamp |
| Velocity non-negative and plausible | Prevent corrupt speed metrics | Flag suspicious values; do not silently clamp |
| Altitude nullable | Source may omit altitude | Preserve null rather than invent zero |
| Duplicate event detection | Polling can repeat observations | Use deterministic event identity and dedupe policy |
| Callsign normalized | Avoid grouping whitespace/case variants | Trim and uppercase; preserve null |

A DLQ should preserve the original payload, reason, ingestion timestamp and source context. It should not become a black hole: monitor its volume and periodically inspect records.

## 7. Deduplication and idempotency

Polling snapshots can repeat the same aircraft state. A deterministic event ID might be derived from stable fields such as `icao24`, source event timestamp and relevant state fields. The exact identity rule must be documented: two legitimate observations can share a timestamp but differ in other fields.

Kafka producer retries, Spark checkpoints and Parquet append mode do not automatically create end-to-end exactly-once business results. A robust design needs:
1. deterministic event identity;
2. a documented duplicate policy;
3. replay tests;
4. idempotent or replaceable sink writes, or a downstream deduplication step;
5. checkpoint lifecycle discipline.

## 8. Parquet, DuckDB and dbt

**Parquet** is a columnar file format. It is efficient for analytical scans because readers can avoid loading unrelated columns and can benefit from compression and metadata. Partition output by useful dimensions only when query patterns and file sizes justify it; too many tiny partitions/files can hurt performance.

**DuckDB** is an embedded analytical database. It can query Parquet directly and is well suited to local analytics without operating a separate database server. It is not a message broker or a distributed streaming engine.

**dbt Core** organizes SQL transformations into a dependency graph. Staging models standardize source records; marts encode reusable business definitions. Tests can assert non-nullness, uniqueness, accepted values and relationships. dbt does not ingest OpenSky data or replace Kafka/Spark.

Current compatibility issue: the existing SQL uses BigQuery types/functions and BigQuery model configuration. To use DuckDB, port the SQL, declare sources correctly, install `dbt-duckdb`, point it at the actual Parquet/DuckDB data, and run `dbt debug`, `dbt run` and `dbt test`. Do not say DuckDB dbt models are working until those commands pass.

## 9. Analytics questions Airlytics can eventually answer

With validated pings and sessions, useful questions include:
- How many unique aircraft were observed in a time window?
- What share of observed aircraft were airborne versus on ground?
- Which regions have the highest observed ping density?
- What are average and maximum observed speeds/altitudes?
- How does observed traffic vary by hour?
- Which aircraft have the most observations?
- How many telemetry sessions were detected under the inactivity-gap rule?
- How fresh is the data from source observation to dashboard display?
- Which API polls failed, returned no states, or were rate-limited?

Be careful with interpretation:
- A state-vector snapshot is not a scheduled flight.
- A country field is not necessarily the aircraft's current location or operator.
- Nearest-airport heuristics are not origin/destination inference.
- A 15-minute gap session is not necessarily a real-world flight.
- Synthetic fallback data must be labelled clearly and never mixed with live metrics without a visible source indicator.

## 10. Metrics to add

**Ingestion:** poll count, HTTP status counts, latency p50/p95, records received, valid records, DLQ count, 429 count, retry count.

**Streaming:** input rows/sec, processing rows/sec, batch duration, Kafka lag, late-row count, malformed JSON count, active state size.

**Storage:** Parquet file count/size, rows written, duplicate rate, schema version.

**Analytics:** unique aircraft, airborne count, average speed, session count, session duration distribution, airport-proximity counts.

Every metric should have a clear numerator, denominator, time window, unit and source. Avoid fabricated performance figures.
