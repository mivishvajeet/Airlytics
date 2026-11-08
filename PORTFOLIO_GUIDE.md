# Airlytics — Portfolio & Technical Interview Guide

This guide provides high-impact talking points, deep-dive architectural Q&A, and resume bullet points for presenting **Airlytics** in Data Engineering, Cloud Infrastructure, and Distributed Systems interviews.

---

## 1. The 30-Second Elevator Pitch

> *"Airlytics is an end-to-end, real-time flight tracking and airspace analytics platform built on Google Cloud. It ingests high-frequency ADS-B transponder telemetry from the OpenSky Network, buffers it through Cloud Pub/Sub, and uses Apache Beam on Cloud Dataflow with 15-minute gap session windows to reconstruct discrete flight legs from raw position reports. Downstream, it leverages BigQuery GIS to evaluate 5km geodesic airport buffers for takeoff and landing detection, with Dataform managing incremental ELT transformations. The entire system is codified with modular Terraform and deployed using zero-key Workload Identity Federation (WIF) with automated cost guardrails."*

---

## 2. Resume Ready Bullet Points

You can copy and adapt these bullet points for your resume:

- **Architected a real-time streaming pipeline on Google Cloud** ingesting live ADS-B flight transponder telemetry from OpenSky Network, processing 1,000+ events/sec with sub-2-minute queryable latency and <0.1% deduplication error.
- **Engineered an Apache Beam streaming pipeline on Cloud Dataflow** featuring 15-minute gap session windowing to reconstruct discrete flight legs, late-data routing via side outputs, and bounded worker auto-scaling for cost control.
- **Optimized BigQuery storage and scan efficiency** using daily event-time partitioning and multi-column clustering (`icao24`, `callsign`), reducing analytic scan overhead by >95%.
- **Implemented spatial geospatial intelligence with BigQuery GIS**, performing `ST_CONTAINS` containment joins against pre-computed 5,000m geodesic airport buffer polygons to classify takeoff and landing kinematics.
- **Automated multi-environment infrastructure provisioning** using modular Terraform (`dev`/`prod`), establishing Cloud Billing budget alerts, GCS lifecycle TTLs, and Cloud Monitoring 10-minute silence freshness policies.
- **Secured zero-key CI/CD pipelines in GitHub Actions** utilizing Workload Identity Federation (OIDC) token exchange, eliminating persistent service account JSON keys.
- **Built an interactive real-time Streamlit dashboard** featuring live Plotly/Mapbox radar feeds, altitude distribution histograms, and airport congestion matrices.

---

## 3. Top Technical Interview Questions & Model Answers

### Q1: Why did you use Session Windows instead of Fixed/Tumbling Windows in Apache Beam?
**Model Answer**:
> *"Aircraft transponders broadcast discrete, irregular pings. Unlike batch logs, a flight leg doesn't conform to an arbitrary 1-hour calendar boundary. If you use a 1-hour tumbling window, a 90-minute flight between London and Paris gets arbitrarily chopped in half, corrupting origin, destination, duration, and maximum altitude metrics.*
>
> *Session windowing (`window.Sessions(gap_size=15 minutes)`) keys on the aircraft's `icao24` airframe address. It dynamically keeps a session open as long as pings continue arriving. When an aircraft lands, taxis, or shuts off its transponder at the gate, 15 minutes of silence elapses, and Beam automatically fires and materializes the completed flight leg."*

---

### Q2: How does your streaming architecture handle late-arriving and out-of-order data?
**Model Answer**:
> *"Network latency, ground receiver disconnections, and API buffering can cause telemetry to arrive minutes after transponder broadcast. In Beam, I separated **Event Time** (the transponder's actual broadcast timestamp) from **Processing Time** (the ingestion time).*
>
> *I configured an **allowed lateness** threshold of 10 minutes with accumulation mode set to `DISCARDING`. Pings arriving within the 10-minute boundary are incorporated into their session window. Pings arriving beyond the allowed lateness are emitted to a tagged **side-output sink** that lands in `raw_flights.late_pings`. This guarantees that late data is preserved for auditability and SLA tracking without stalling or retroactively invalidating primary streaming aggregations."*

---

### Q3: Why perform the 5km airport geofencing in BigQuery GIS rather than inside the Apache Beam pipeline?
**Model Answer**:
> *"There are over 70,000 global airports. Evaluating geospatial point-in-polygon checks in stream processing requires loading all airport polygon boundaries into worker memory or using external spatial indexing libraries like JTS or Uber H3, which introduces memory bloat, high worker footprints, and serialization overhead on Dataflow.*
>
> *BigQuery GIS natively supports spherical geodesic geometry (`GEOGRAPHY` type). By pre-calculating the 5km buffer polygons (`ST_BUFFER(airport_point, 5000.0)`) in `curated_flights.dim_airport` and performing spatial containment joins (`ST_CONTAINS`) incrementally in BigQuery SQL via Dataform, the database engine leverages its internal S2 spherical indexing. This executes spatial queries in parallel across massive datasets with sub-second performance at a fraction of the compute cost."*

---

### Q4: How did you design BigQuery partitioning and clustering, and why?
**Model Answer**:
> *"In flight tracking, telemetry writes are high-volume and append-heavy, while analytical queries typically filter by a specific aircraft (`icao24`) or callsign over recent operating windows.*
>
> *I partitioned `raw_flights.raw_pings` **daily by `DATE(event_timestamp)`** with a 90-day partition expiration policy. This bounds table growth and limits scan scope. Then I clustered by `(icao24, callsign)`. Because BigQuery colocates rows with matching cluster keys within partition blocks, queries filtering for a single flight leg or tail number prune 95% of scanned data blocks, slashing on-demand query costs and drastically improving latency."*

---

### Q5: How did you ensure zero credential leakage and zero static keys in CI/CD?
**Model Answer**:
> *"Storing Google Cloud service account JSON keys inside GitHub Secrets is a major security vulnerability—keys are static, long-lived, difficult to audit, and susceptible to leakage.*
>
> *I implemented **Workload Identity Federation (WIF)** using OpenID Connect (OIDC). In `.github/workflows/cd.yml`, GitHub Actions requests a short-lived OIDC JWT token signed by GitHub. Google Cloud's Security Token Service validates the token's claims (repository, branch, workflow) and impersonates a designated deployment service account for that specific run. Tokens expire within minutes, and no static credentials ever exist on disk or in secret storage."*

---

### Q6: How did you implement FinOps and prevent runaway cloud billing?
**Model Answer**:
> *"For a streaming cloud architecture, uncontrolled streaming worker scaling or unmonitored queries can quickly generate unexpected charges. I embedded five layers of cost guardrails in Terraform:*
>
> 1. *Dataflow max workers were hard-capped to 2 (`max_num_workers = 2`).*
> 2. *Cloud Storage buckets for Dataflow temp and staging were provisioned with 7-day TTL lifecycle deletion policies.*
> 3. *Cloud Run ingestion jobs were scheduled via Cloud Scheduler to run during active daytime corridors rather than 24/7.*
> 4. *Declared an automated Google Cloud Billing Budget resource with alert notifications at 50%, 80%, and 100% of the $25 monthly budget.*
> 5. *BigQuery partition expirations automatically clean up raw partitions older than 90 days."*

---

### Q7: If flight volume scaled 100x (Global airspace, 100,000+ aircraft), how would you evolve the architecture?
**Model Answer**:
> *"1. **Ingestion**: Replace periodic REST polling with direct multi-region ADS-B receiver feeds or high-throughput Kafka / Confluent Cloud clusters partitioned by geographic S2 airspace cells.*
> *2. **Stream Processing**: Scale Dataflow with Streaming Engine enabled and utilize BigQuery's Storage Write API (instead of legacy streaming inserts) to achieve higher throughput at lower per-row ingestion costs.*
> *3. **Materialization**: Introduce BigQuery BI Engine in-memory caching and Materialized Views for top-flight corridors (`fact_route_agg`) to serve dashboard requests with single-digit millisecond latency without running SQL scans."*

---

## 4. Key Metrics & SLA Cheat Sheet

| Metric | Target | How Airlytics Achieves It |
| :--- | :--- | :--- |
| **End-to-End Latency** | < 2 minutes | Pub/Sub buffer + Dataflow streaming write to BigQuery |
| **Deduplication Rate** | < 0.1% duplicates | Unique surrogate keys (`MD5(icao24 \|\| timestamp)`) + Dataform incremental merges |
| **Ingestion Freshness** | Alert on 10m silence | Cloud Monitoring metric absence alert policy (`duration = "600s"`) |
| **Dev Environment Cost** | < $25 / month | Worker caps (2), GCS 7-day TTL, partition expiration (90d), budget alert triggers |
| **Security SLA** | 0 static keys | 100% Workload Identity Federation OIDC token exchange |
