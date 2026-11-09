# 08 — Resume claims and validation checklist

## 1. Resume wording: safe version for the current state

Use bullets that accurately describe implemented code and clearly avoid unmeasured performance claims. Edit the wording to match your personal contribution.

- Built a Python-based ingestion pipeline for OpenSky aircraft state-vector data, including response parsing, normalization, configurable polling and error handling.
- Implemented Kafka-based event transport with separate topics for valid flight telemetry and rejected records, using aircraft identifiers as message keys.
- Developed a PySpark Structured Streaming workflow for event-time processing, inactivity-based aircraft sessions and Parquet output.
- Designed a local analytics stack around Parquet, DuckDB, dbt Core and Streamlit to support reusable SQL transformations and flight-telemetry visualizations.
- Kept the target architecture local-first to avoid dependency on paid cloud infrastructure; retained earlier cloud deployment components as legacy reference.

**Important:** If a component has not passed a local run, change "implemented/developed" to "designed/scaffolded" or remove it. The existence of a file is not evidence of a working pipeline.

## 2. Stronger bullets only after validation

After measuring actual results, you may add evidence such as:
- Processed **[measured]** events per second on **[hardware/runtime]** with **[measured]** end-to-end latency.
- Achieved **[measured]%** valid-record rate across **[N]** API polls and routed **[N]** malformed records to a DLQ.
- Reduced duplicate observations by **[measured]%** using **[documented event identity and deduplication rule]**.
- Built **[N]** tested dbt models with **[N]** passing data-quality assertions.
- Queried **[N]** Parquet rows in DuckDB in **[measured time]** on **[machine/specification]**.

Never invent throughput, latency, row counts, cost savings or accuracy. Capture command output and test evidence before using numbers.

## 3. One-minute interview monologue

" I built Airlytics to explore how raw flight telemetry can be turned into useful analytical data. The source is the OpenSky state-vector API, which returns observations with optional and sometimes missing fields. I wrote a Python ingestion path to poll the API, normalize the positional response into named fields, and separate valid records from rejected ones. The valid events are published to Kafka, keyed by aircraft identifier, so ingestion and downstream processing are decoupled.

The streaming layer uses PySpark Structured Streaming to parse the events, work with event timestamps and group observations into sessions using an inactivity gap. The output is stored as Parquet for analytical querying. My target analytical layer is DuckDB and dbt, with Streamlit for visualization. I chose local tools to keep the project reproducible and avoid cloud spend.

One important design limitation is that an inactivity-based telemetry session is not necessarily a real airline flight. I am also validating the integration between Spark output, DuckDB/dbt and the dashboard. I prefer to call out those limitations clearly rather than claim an end-to-end production system before I have evidence."

Remove the spaces immediately inside the opening and closing quotation marks when speaking; this is a draft script, not a required memorized answer.

## 4. Project demo checklist

### Source and ingestion
- [ ] Show the OpenSky API documentation and explain the state-vector fields.
- [ ] Show one captured response/fixture.
- [ ] Explain null handling and coordinate validation.
- [ ] Demonstrate event timestamp versus ingestion timestamp.
- [ ] Show retry behavior for 429/5xx/timeouts using mocks.
- [ ] Show one valid Kafka message and one DLQ record.

### Kafka
- [ ] Start Compose and show healthy broker status.
- [ ] Explain host listener `localhost:19092` versus container listener `kafka:9092`.
- [ ] Show topics, keys and sample messages.
- [ ] Explain why the single-broker setup is not highly available.

### Spark
- [ ] Show Spark/Java/Scala/connector versions.
- [ ] Run a bounded Kafka-to-Parquet test.
- [ ] Show a streaming query progress snapshot.
- [ ] Demonstrate event-time handling and a late event.
- [ ] Restart with the same checkpoint and verify behavior.
- [ ] Replay data and inspect duplicates.

### Storage and analytics
- [ ] Confirm raw and curated paths have clear meanings.
- [ ] Query Parquet using DuckDB.
- [ ] Install and run dbt-duckdb.
- [ ] Confirm all sources resolve.
- [ ] Run dbt models and data tests successfully.
- [ ] Explain the grain of every mart.

### Dashboard
- [ ] Fix provider/poller method and field mismatches.
- [ ] Connect dashboard to persisted DuckDB/Parquet data.
- [ ] Display live, fixture and synthetic source modes distinctly.
- [ ] Confirm charts use the documented units and timezone.

### Cost and repository
- [ ] Confirm no cloud resource is required to run the project.
- [ ] Confirm no secrets are committed.
- [ ] Confirm GitHub Actions workflows are manual-triggered.
- [ ] Review GitHub Actions usage/billing before running workflows.
- [ ] Confirm legacy cloud directories are clearly labelled and not deployed.

## 5. Commands to record (run from repository root)

Exact launch commands may evolve; verify the README and scripts before using them.

```bash
# Python tests
PYTHONPATH=. python -m pytest -q ingest/tests

# Full configured test targets
PYTHONPATH=. python -m pytest -q ingest/tests spark/tests tests

# Local services
docker compose config
docker compose up -d kafka kafka-ui
docker compose ps
docker compose logs -f kafka

# dbt checks after DuckDB port is complete
cd dbt_airlytics
dbt debug
dbt run
dbt test
```

For Spark, record the exact `spark-submit` command, connector artifact, Java version, Spark version and output/checkpoint directories. Do not copy an unverified command into a resume or interview answer.

## 6. Evidence record

Keep a small evidence log with:
- date and Git commit;
- operating system and hardware;
- Python, Java, Spark, Kafka, dbt and DuckDB versions;
- exact command;
- input record count;
- output record count;
- test result;
- observed latency/throughput, including measurement method;
- known limitations.

A credible project explanation is stronger when it includes one real failure, how you diagnosed it, and the evidence that the fix worked.

## 7. Final rule

Do not present the target architecture as a fully operational platform until the end-to-end demo checklist passes. A clear explanation of what works, what does not, and how you validated it is a stronger interview signal than inflated metrics.
