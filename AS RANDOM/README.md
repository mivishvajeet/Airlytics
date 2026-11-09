# Airlytics Engineering & Interview Handbook

This handbook explains the Airlytics project from both a **data engineering** and **data analytics** perspective. It is written to help maintain the codebase, debug the pipeline, and explain design decisions accurately in interviews.

## Read in this order

1. [Project overview and architecture](01-project-overview.md) — problem statement, scope, architecture, data flow, and current implementation status.
2. [File-by-file code guide](02-file-by-file-guide.md) — the purpose of each important module and configuration file.
3. [Data engineering and analytics concepts](03-data-engineering-and-analytics.md) — schema, data quality, event time, sessions, Parquet, analytical marts, and metrics.
4. [Technology choices and alternatives](04-technology-decisions.md) — why each tool was selected and when alternatives would be better.
5. [Reliability, OpenSky API and cost](05-reliability-api-and-cost.md) — rate limits, retry policy, failure modes, checkpointing, local resource management, and the $0 requirement.
6. [Known gaps and troubleshooting](06-known-gaps-and-troubleshooting.md) — verified code-level mismatches, unvalidated behavior, and a practical remediation order.
7. [Interview playbook](07-interview-playbook.md) — project pitch, likely recruiter questions, technical follow-ups, sample answer frameworks, and exercises.
8. [Resume claims and validation checklist](08-resume-and-validation-checklist.md) — safe resume wording and evidence required before making stronger claims.

## How to use this handbook

- Read the overview first, then follow the data from source to dashboard.
- Open the linked source file while reading the file guide.
- Rehearse interview answers in your own words; do not memorize claims you cannot demonstrate.
- Update the handbook when code changes.
- Treat status labels as important: **implemented in code**, **partially implemented**, **legacy**, **planned**, and **not yet validated** mean different things.

## Evidence and honesty policy

This handbook distinguishes repository inspection from execution. A function being present in source code does not prove that the end-to-end pipeline runs. Unless a test or local run is explicitly recorded, behavior is described as intended or statically inspected, not runtime-verified.

The active project constraint is **no paid infrastructure and no required cloud deployment**. All core processing should run locally. OpenSky remains an external service whose availability, access rules and request limits are outside this repository's control.

## Official references

- [OpenSky REST API](https://openskynetwork.github.io/opensky-api/rest.html)
- [Apache Kafka concepts](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [DuckDB Parquet guide](https://duckdb.org/docs/data/parquet/overview)
- [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)
- [GitHub Actions usage limits](https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions)
