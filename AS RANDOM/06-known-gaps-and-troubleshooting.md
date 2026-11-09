# 06 — Known gaps and troubleshooting

## Status vocabulary

- **Implemented in source**: code/configuration exists.
- **Partially implemented**: a component exists but integration or behavior is incomplete.
- **Legacy**: retained from the earlier cloud architecture, not active in the local target.
- **Planned**: a design intention that is not yet implemented.
- **Not validated**: no successful runtime/test evidence has been recorded here.

A static code review can identify likely defects; only tests and local execution can confirm runtime behavior.

## Current high-priority gaps

### 1. Dashboard calls the wrong poller method
Static review found `dashboard/data_provider.py` calls `poll_once()`, while `ingest/main.py` uses `poll_cycle()` and the poller API should be checked for the exact available method. This can break live dashboard retrieval. Align the method call with the real poller interface and add a unit test for the provider.

### 2. Dashboard field names may not match the canonical model
The provider reads attributes such as `ping_id` and `altitude_baro_m`. Confirm these names against `NormalizedFlightPing`. Prefer one canonical schema and a mapping function at the boundary instead of scattered aliases.

### 3. Dashboard silently falls back to synthetic data
The app catches live-data exceptions and returns synthetic pings. That is useful for UI development but can mislead users into thinking the data is live. Show a visible source badge, log the fallback reason, and provide a strict live-only mode for validation.

### 4. dbt SQL is not yet DuckDB-compatible
The staging and mart models use BigQuery-specific types/functions/configuration such as `INT64`, `FLOAT64`, `ST_GEOGPOINT`, `QUALIFY`, `TIMESTAMP_DIFF`, `UNIX_SECONDS`, partitioning and clustering config. Port to DuckDB SQL, add `dbt-duckdb`, declare source tables correctly and run `dbt debug`, `dbt run`, `dbt test`.

### 5. dbt source and physical input need to be defined
The staging model references `source('raw_flights', 'raw_pings')`. Ensure a matching `sources.yml` exists and points to an actual DuckDB table/view or external Parquet relation. A profile alone does not create or load the source relation.

### 6. Sessionization has two potential owners
Spark computes session aggregates; dbt's fact model also reconstructs sessions. Choose one authoritative definition. If Spark owns sessionization, dbt should transform the persisted session output into downstream marts. If dbt owns it, Spark should land normalized pings rather than a duplicate session result.

### 7. Output layer naming is misleading
The Spark default output path is `data/bronze/flight-legs`, but flight-leg/session aggregates are not raw bronze records. Rename the output and document layer contracts. Avoid changing the path until checkpoint migration and existing local data implications are understood.

### 8. Event identity and deduplication need a contract
The normalized model, Kafka message key and downstream model use different concepts (aircraft key, message ID, ping ID). Define:
- deterministic event ID;
- whether same aircraft + same event timestamp is a duplicate;
- how conflicting records are resolved;
- whether source time or ingestion time wins;
- how replay is handled.

A Kafka key of `icao24` does not deduplicate records.

### 9. Spark Kafka connector must match runtime
PySpark installation alone may not include the Kafka source connector JAR. Use a connector artifact compatible with Spark and Scala versions. Verify startup with a small local query before launching the full aggregation.

### 10. CI coverage is narrow
The manual CI workflow installs ingestion dependencies and runs ingestion tests only. It does not validate Docker Compose, Kafka publishing, Spark processing, Parquet output, DuckDB/dbt models or the dashboard. Add separate integration checks only when they can run reliably and affordably.

## Troubleshooting guide

| Symptom | Likely area | What to inspect |
|---|---|---|
| Connection refused on 19092 | Kafka not running/listener mismatch | `docker compose ps`, Kafka logs, advertised listener |
| Container can’t connect to Kafka | Wrong bootstrap address | Use `kafka:9092` inside Compose network, not localhost |
| Host app can’t connect | Wrong host listener | Use `localhost:19092` |
| Topic not found | Topic auto-creation/config | List topics and create expected topics if needed |
| Producer appears successful but topic empty | Async delivery/flush | Check delivery future, callback, flush and broker logs |
| Spark cannot find Kafka source | Missing connector JAR | Check `--packages` artifact and Spark/Scala compatibility |
| Spark parses null records | Schema mismatch or malformed JSON | Inspect raw Kafka value and schema field names |
| No Parquet files | Query not started or no completed sessions | Inspect Spark progress, watermark, output mode and session gap |
| Duplicate Parquet rows | Replay/append semantics | Inspect checkpoint and event identity/dedup policy |
| dbt says adapter not found | Wrong Python environment | Install `dbt-duckdb` where dbt command runs |
| dbt source not found | Missing source declaration/relation | Inspect `sources.yml` and DuckDB input setup |
| dbt SQL compilation error | BigQuery dialect | Port functions/types/configuration |
| Dashboard shows data despite API failure | Synthetic fallback | Check source mode and exception logs |
| Dashboard crashes on attribute error | Model/provider mismatch | Compare dataclass fields with provider mapping |
| Tests fail to import package | Python path/environment | Run from repo root with `PYTHONPATH=.` |

## Recommended remediation order

1. Confirm the canonical normalized ping schema and add schema-contract tests.
2. Fix the dashboard provider method/field mismatches and expose live versus synthetic source state.
3. Make producer delivery confirmation explicit and test Kafka publishing.
4. Confirm the Docker Compose broker is healthy and topic creation is deterministic.
5. Pin Spark, Java, Scala and Kafka connector compatibility.
6. Run a small Spark Kafka-to-Parquet integration test.
7. Define raw versus curated Parquet paths and a stable checkpoint policy.
8. Port dbt models to DuckDB and add source declarations/tests.
9. Connect dashboard to persisted DuckDB/Parquet data.
10. Expand CI carefully without enabling paid cloud deployments or frequent hosted runs.

## Evidence log template

Record real results here after testing:

| Date | Command/test | Environment | Result | Commit |
|---|---|---|---|---|
| YYYY-MM-DD | `pytest -q ingest/tests` | Python version / OS | Pass/fail + count | SHA |
| YYYY-MM-DD | Compose broker smoke test | Docker version | Pass/fail | SHA |
| YYYY-MM-DD | Spark Kafka-to-Parquet test | Spark/Java/Scala | Pass/fail + rows | SHA |
| YYYY-MM-DD | `dbt debug && dbt build` | dbt/adapter versions | Pass/fail | SHA |
| YYYY-MM-DD | Dashboard live-data test | Source mode | Pass/fail | SHA |

Do not fill this table with assumed results. At the time this handbook was written, the full local pipeline had not been runtime-validated.
