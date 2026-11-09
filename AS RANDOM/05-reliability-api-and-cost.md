# 05 — Reliability, OpenSky API and cost management

## 1. External API reality

OpenSky is an external data provider. Airlytics cannot control its availability, response latency, coverage, access policy or request limits. Design the poller as a polite client, not as an unlimited feed.

The API endpoint configured by default is `/api/states/all`. A response contains a timestamp and a `states` array of state vectors. The array fields are positional and many values can be null. A response with no states is not automatically an error: it can mean there were no matching observations for the query.

Use the official API documentation for current authentication, quota/credit and request semantics. Do not hard-code a quota number into this handbook because access policies can change and may vary by account/access tier.

Official reference: https://openskynetwork.github.io/opensky-api/rest.html

## 2. Bounding boxes and polling frequency

A bounding box uses:
- `lamin`: minimum latitude
- `lomin`: minimum longitude
- `lamax`: maximum latitude
- `lomax`: maximum longitude

Smaller geographic areas generally reduce returned data volume, but they do not guarantee a particular API credit cost. The current config defaults to latitude 45–55 and longitude -5–15. This is a Western/Central Europe example region, not the whole world.

The poller default interval is 10 seconds. Before using a shorter interval:
1. Check current OpenSky access guidance.
2. Consider whether the dashboard needs that freshness.
3. Limit the geographic area.
4. Avoid parallel pollers hitting the same endpoint.
5. Cache a response for development instead of repeatedly calling the live API.

Use a conservative interval and stop polling when the application is not being used.

## 3. Authentication and secrets

Credentials are optional environment variables:
- `OPENSKY_USERNAME`
- `OPENSKY_PASSWORD`

Never put credentials in source code, screenshots, README examples or committed `.env` files. Keep `.env.example` free of secrets. If a credential was committed, rotate it; deleting it in a later commit does not remove it from Git history.

Do not assume authentication grants unlimited access. Follow the current terms and account limits.

## 4. Retry and backoff strategy

Transient network failures and server errors can be retried. A useful exponential backoff pattern is:

`delay = min(max_delay, base_delay * (2 ** attempt)) + random_jitter`

Recommended policy:
- 429: respect `Retry-After` when supplied; otherwise back off more aggressively.
- 5xx: retry with capped exponential backoff and jitter.
- Timeout / connection reset: retry a limited number of times.
- 4xx other than rate limiting: generally do not retry blindly; log and surface the error.
- 200 with empty states: successful empty poll, not an outage.
- Malformed payload: record a parsing failure and preserve enough context for diagnosis.

The repository config exposes `MAX_RETRIES` and `BACKOFF_FACTOR`; inspect the exact implementation in `ingest/poller.py` before claiming it implements every recommendation above. Add tests for 429, 5xx, timeout, malformed JSON, empty states and recovery.

## 5. Dead-letter queue (DLQ)

The dead-letter topic is `deadletter-pings`. Use it for records that cannot be safely normalized or processed. Include:
- original payload (or a safe representation);
- failure reason and stage;
- observed/source timestamp if available;
- ingestion timestamp;
- schema/version context.

Track DLQ count and rate. A DLQ is useful only if someone can inspect and replay or resolve its records. Avoid logging passwords, authorization headers or sensitive request data.

## 6. Kafka and Spark recovery

Kafka persists records in the broker's local volume. Spark checkpoints store streaming query progress and state. Keep the checkpoint directory stable across restarts when continuing the same query. Deleting checkpoints can cause records to be reprocessed depending on the starting offsets and sink design.

Do not delete the Kafka volume casually. `make clean` executes `docker compose down -v`, which removes named volumes and therefore local Kafka data. `make down` stops services without removing the named volume.

Exactly-once is not achieved simply by enabling Kafka acknowledgements and Spark checkpoints. Test:
- producer retry after an uncertain acknowledgement;
- Spark restart with an existing checkpoint;
- restart with a deleted checkpoint;
- replay from earliest offsets;
- duplicate event IDs;
- partially written output files;
- rerunning downstream dbt models.

For append-only Parquet, decide how duplicate session output is prevented or reconciled.

## 7. Observability

At minimum, log structured values for each poll:
- poll sequence;
- HTTP status;
- request latency;
- states received;
- valid pings;
- DLQ records;
- retries and backoff;
- timestamp of latest event;
- error category.

For Spark, inspect query progress and batch metrics. Useful measures include input rows/sec, processed rows/sec, batch duration, state-store size, Kafka offsets and sink errors. Avoid logging every raw ping at INFO; it creates noise and increases disk use.

## 8. The zero-cost design

The intended architecture avoids paid cloud runtime services:

| Component | Cost posture |
|---|---|
| Python, PySpark, DuckDB, dbt Core, Streamlit | Open-source software; runs locally |
| Apache Kafka container | No managed-service charge; uses local CPU, memory and disk |
| Docker Compose | Local orchestration; uses local machine resources |
| Parquet files | Local disk usage |
| OpenSky | External service with its own access policy/limits; do not assume unlimited use |
| GitHub Actions | Manual workflow only; hosted runner minutes may count against account allowance and can be billable beyond included usage |
| GCP services | Not required; do not provision for this local architecture |

"$0" means no paid cloud infrastructure is required for the core project. It does not mean electricity, hardware, internet, API availability or every third-party usage policy is literally free.

To protect the constraint:
- Do not run Terraform apply.
- Do not create Dataflow, BigQuery, Pub/Sub, Dataproc or Cloud Run resources.
- Keep GitHub Actions manual-triggered and inspect billing/usage before running many jobs.
- Avoid scheduled CI unless there is a clear need.
- Use local sample fixtures for repeatable development.
- Set bounded run duration for experiments.
- Stop containers when finished.
- Monitor local disk use and remove generated data only when you understand what will be deleted.

GitHub Actions billing reference: https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions

## 9. Failure-mode checklist

| Failure | Detection | Response |
|---|---|---|
| OpenSky 429 | HTTP status/log | Respect retry guidance; reduce poll rate |
| OpenSky 5xx | HTTP status/log | Capped retry/backoff |
| Empty state list | 200 with zero states | Record empty poll; do not fabricate live rows |
| Invalid state vector | Parser error | DLQ with reason |
| Kafka unavailable | Producer connection/delivery error | Retry boundedly; stop or buffer safely |
| Spark connector mismatch | Class/package error at startup | Align Spark/Scala/connector versions |
| Bad JSON/schema drift | Parse failures/null parsed struct | Quarantine and version schema |
| Checkpoint corruption/mismatch | Spark query startup/state error | Preserve copy; diagnose before reset |
| Disk full | OS/container write error | Stop pipeline, inspect volume/output sizes |
| dbt adapter missing | dbt adapter/profile error | Install dbt-duckdb in correct environment |
| BigQuery SQL on DuckDB | SQL compilation error | Port SQL and test model by model |
| Dashboard uses synthetic fallback | Source indicator/log | Display source mode prominently; don't present synthetic metrics as live |

## 10. Operational runbook (verify commands against current README)

1. Copy `.env.example` to a local `.env` if the launcher supports it; otherwise export variables in the shell.
2. Start the broker: `make up`.
3. Check containers: `make ps`.
4. Inspect Kafka UI at `http://localhost:8080`.
5. Run ingestion with the documented local command and a bounded duration.
6. Confirm records appear in `raw-flight-pings`; inspect DLQ separately.
7. Start Spark with the correct Kafka connector package and stable checkpoint/output paths.
8. Inspect Parquet output using DuckDB.
9. Run dbt only after DuckDB SQL and source configuration are ported.
10. Start Streamlit only after its provider reads the intended persisted dataset.
11. Stop services with `make down`. Use `make clean` only when intentionally deleting Kafka volume data.

This runbook is a sequence to validate, not evidence that every step currently succeeds.
