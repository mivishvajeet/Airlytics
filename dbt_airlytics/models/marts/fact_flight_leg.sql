{{
    config(
        materialized='table',
        partition_by={
            "field": "session_start",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['icao24', 'callsign']
    )
}}

-- In Dataproc/Spark streaming mode, fact_flight_leg is populated directly by PySpark.
-- In dbt batch mode, this model reconstructs legs from stg_raw_pings if needed.
WITH pings AS (
    SELECT * FROM {{ ref('stg_raw_pings') }}
),

ranked AS (
    SELECT
        message_id,
        icao24,
        callsign,
        origin_country,
        event_timestamp,
        altitude_baro_m,
        velocity_ms,
        on_ground,
        ping_point,
        LAG(event_timestamp) OVER(PARTITION BY icao24 ORDER BY event_timestamp) AS prev_timestamp
    FROM pings
),

gaps AS (
    SELECT
        *,
        CASE
            WHEN prev_timestamp IS NULL THEN 1
            WHEN TIMESTAMP_DIFF(event_timestamp, prev_timestamp, MINUTE) >= 15 THEN 1
            ELSE 0
        END AS is_new_session
    FROM ranked
),

sessions AS (
    SELECT
        *,
        SUM(is_new_session) OVER(PARTITION BY icao24 ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),

legs AS (
    SELECT
        CONCAT(icao24, '_', CAST(session_id AS STRING), '_', CAST(UNIX_SECONDS(MIN(event_timestamp)) AS STRING)) AS flight_leg_id,
        icao24,
        MAX(callsign) AS callsign,
        MAX(origin_country) AS origin_country,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        ROUND(TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) / 60.0, 1) AS duration_minutes,
        COUNT(*) AS ping_count,
        MAX(altitude_baro_m) AS max_altitude_m,
        ROUND(AVG(velocity_ms), 1) AS avg_velocity_ms
    FROM sessions
    GROUP BY icao24, session_id
)

SELECT * FROM legs
WHERE duration_minutes >= 1.0

