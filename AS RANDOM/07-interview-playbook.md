# 07 — Interview playbook

This chapter helps explain Airlytics clearly without overstating its maturity. Adapt the answers to what you have personally run and can demonstrate.

## 1. 30-second project pitch

"Airlytics is a local-first data engineering project that turns aircraft state-vector observations from the OpenSky API into analytics-ready datasets. A Python ingestion service polls and normalizes the API response, publishes valid events to Kafka and routes rejected records to a dead-letter topic. PySpark Structured Streaming is designed to consume the events, apply event-time sessionization and write Parquet output. DuckDB and dbt form the local analytical layer, with Streamlit as the dashboard. I chose this stack so I could demonstrate streaming and analytics patterns without depending on paid cloud infrastructure. I am validating the end-to-end integrations and have documented the current gaps rather than presenting untested components as production-ready."

## 2. Two-minute explanation

Start with the problem: OpenSky returns repeated, partially populated state vectors rather than a clean flight analytics table. Explain the pipeline in layers:
1. **Ingestion:** Python handles HTTP, bounding-box parameters, timeouts, retries, parsing and normalization.
2. **Transport:** Kafka decouples the poller from downstream processing. The aircraft identifier is the message key; invalid records go to a DLQ.
3. **Processing:** Spark reads Kafka JSON, uses event time and a watermark, and groups observations into inactivity sessions.
4. **Storage:** Parquet provides a columnar analytical output.
5. **Analytics:** DuckDB queries local files; dbt is intended to standardize models and create marts.
6. **Presentation:** Streamlit visualizes operational and traffic metrics.

Then discuss trade-offs: local Kafka is not highly available; Spark is heavier than a simple Python consumer; OpenSky is rate-limited; a telemetry session is not a true flight leg. Finish with tests you ran and the most important remaining gap.

## 3. Recruiter / hiring-manager questions

**Q: What problem does it solve?**  
A: It demonstrates how to transform raw aircraft telemetry observations into structured, time-based data that can be analyzed. It is a portfolio/learning platform, not an operational air-traffic product.

**Q: What was your contribution?**  
A: Be precise about which modules you personally designed, implemented, tested and debugged. Explain the code, not only the tool names.

**Q: Why did you build it locally?**  
A: To keep the project reproducible and avoid cloud spend while learning distributed-data concepts. Local execution is a deliberate constraint, not a claim of production-scale capacity.

**Q: What was the hardest part?**  
A: A strong answer can cover positional/null-rich API data, event-time versus ingestion-time, duplicate polling observations, Kafka/Spark compatibility, or reconciling cloud-era code with a local architecture. Use a real example and explain the diagnosis.

**Q: Is it production-ready?**  
A: No. It is a local portfolio project. The remaining work includes full integration validation, DuckDB SQL porting, dashboard source integration, and replay/idempotency testing.

**Q: What would you improve next?**  
A: Define an event schema and deterministic event ID, add integration tests, make sink writes replay-safe, complete dbt models, expose data freshness/DLQ metrics and document measured throughput.

## 4. Data engineering technical questions

**Q: Why use Kafka?**  
A: It decouples ingestion from processing and provides a durable partitioned event log. Producers and consumers can progress independently. It also introduces operational concepts such as partitions, offsets and consumer groups.

**Q: Why use a message key?**  
A: Keying by `icao24` routes one aircraft's records to the same partition under the partitioning strategy, preserving per-partition order for that key. It does not provide global ordering or deduplication.

**Q: What is a dead-letter queue?**  
A: A separate destination for records that cannot be safely processed, with the original payload and error context preserved for inspection and replay.

**Q: What is the difference between event time and processing time?**  
A: Event time is when the source says the observation occurred; processing time is when the engine handles it. Late data makes this distinction important for correct time-window analytics.

**Q: What does a watermark do?**  
A: It bounds how long Spark retains state for event-time operations and defines a lateness threshold for stateful computations. It is not a guarantee that all late records are retained.

**Q: What is sessionization?**  
A: Grouping events for a key into sessions separated by an inactivity gap. In Airlytics, a 15-minute gap is a heuristic over telemetry observations, not a verified airline flight boundary.

**Q: Does Kafka + Spark checkpointing guarantee exactly-once?**  
A: Not end-to-end by itself. The sink and replay behavior matter. I need deterministic event identity, idempotent writes or downstream deduplication, and replay tests.

**Q: How would you handle schema evolution?**  
A: Version the event schema, make additive nullable fields backward-compatible where possible, validate producers/consumers against a contract, and quarantine incompatible payloads. Plan explicit migrations for breaking changes.

**Q: How do you handle duplicates?**  
A: Define a deterministic identity based on aircraft and source event data, decide how conflicting observations are resolved, deduplicate before aggregation or in a downstream model, and test replay.

**Q: Why Parquet?**  
A: It is columnar, compressed and efficient for analytical scans. It is also readable by Spark and DuckDB, which avoids coupling storage to one engine.

## 5. Data analytics and SQL questions

**Q: What is the difference between a fact table and a dimension table?**  
A: A fact table stores measurable events or aggregates at a defined grain. A dimension table stores descriptive attributes used to filter/group facts. Airlytics could have a ping fact at one row per observation and aircraft/airport dimensions if those entities are reliably sourced.

**Q: What is the grain of a flight-session fact?**  
A: One row per aircraft telemetry session under the configured inactivity-gap rule. State this explicitly; do not call it one row per real-world flight unless independently validated.

**Q: Why deduplicate before aggregation?**  
A: Repeated observations inflate counts and can distort averages and durations. The dedupe key and conflict rule must reflect the source semantics.

**Q: How would you calculate average speed by hour?**  
A: Convert event timestamps to the chosen timezone, truncate to hour, group by hour, and average velocity. Clarify whether the metric is per-ping average or first averaged per-aircraft to avoid aircraft with more pings dominating.

**Q: How would you find aircraft with the most observations?**  
A: Group by `icao24`, count rows, order descending, and limit. Use a defined time range and deduped input.

**Q: How would you calculate data freshness?**  
A: Measure ingestion timestamp minus event timestamp, report a distribution (median/p95), and exclude or separately classify missing timestamps and clock-skew anomalies.

**Q: Why use dbt?**  
A: It makes SQL transformations modular, version-controlled and dependency-aware, and supports tests and documentation. It is not the ingestion or streaming engine.

## 6. Scenario questions

**Scenario: OpenSky starts returning 429.**  
Reduce request frequency, respect `Retry-After`, apply capped backoff with jitter, avoid parallel pollers, and log the rate-limit event. Do not retry aggressively.

**Scenario: Kafka is down but the poller is still receiving data.**  
Do not silently discard events. Fail visibly or introduce a bounded local buffer with a documented durability policy. Alert/log broker delivery failures and resume safely after recovery.

**Scenario: Spark restarts and writes duplicate Parquet rows.**  
Inspect checkpoint continuity and sink semantics. Use deterministic IDs and a dedupe/reconciliation layer or an idempotent sink design. Reproduce with a restart/replay test.

**Scenario: dashboard metrics look healthy but API is down.**  
Check whether synthetic fallback data is being displayed. Show the source mode and fallback reason clearly. Never label generated data as live.

**Scenario: dbt fails with a SQL syntax error.**  
Check adapter, profile and dialect. The current models use BigQuery-specific functions; port them to DuckDB rather than assuming SQL dialects are identical.

## 7. Questions to ask yourself before an interview

- Can I draw the architecture without looking at the README?
- Can I explain every field in the canonical event schema?
- Can I show one real API response and trace it through normalization?
- Can I demonstrate a valid Kafka message and a DLQ message?
- Can I explain partition key, offset and consumer checkpoint?
- Can I explain the watermark and session-gap trade-off?
- Can I query the Parquet output with DuckDB?
- Can I run dbt successfully against the actual local source?
- Can I prove whether dashboard data is live or synthetic?
- Can I state the tests run, versions used and measured results honestly?

## 8. A strong answer structure

Use **Problem -> Design -> Trade-off -> Evidence -> Improvement**:
- Problem: what was difficult about the data?
- Design: what did you implement and why?
- Trade-off: what did the choice cost or limit?
- Evidence: what command, test, log or output proves it?
- Improvement: what would you change next?

Avoid reciting a list of technologies without explaining the data flow.
