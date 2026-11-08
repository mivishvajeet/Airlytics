-- Fails if telemetry stream has been silent for > 10 minutes (600 seconds)
WITH latest_ping AS (
    SELECT
        MAX(event_timestamp) AS max_event_timestamp
    FROM {{ ref('stg_raw_pings') }}
),

freshness AS (
    SELECT
        max_event_timestamp,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), max_event_timestamp, MINUTE) AS latency_minutes
    FROM latest_ping
)

SELECT *
FROM freshness
WHERE latency_minutes > 10

