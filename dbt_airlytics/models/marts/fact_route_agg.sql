{{
    config(
        materialized='table',
        partition_by={
            "field": "operating_date",
            "data_type": "date"
        },
        cluster_by=['callsign']
    )
}}

WITH legs AS (
    SELECT * FROM {{ ref('fact_flight_leg') }}
)

SELECT
    DATE(session_start) AS operating_date,
    EXTRACT(HOUR FROM session_start) AS departure_hour,
    callsign,
    origin_country,
    COUNT(flight_leg_id) AS total_flights,
    ROUND(AVG(duration_minutes), 1) AS avg_duration_minutes,
    ROUND(MIN(duration_minutes), 1) AS min_duration_minutes,
    ROUND(MAX(duration_minutes), 1) AS max_duration_minutes,
    ROUND(AVG(avg_velocity_ms), 1) AS avg_speed_mps,
    ROUND(MAX(max_altitude_m), 0) AS max_flight_altitude_m
FROM legs
GROUP BY
    operating_date,
    departure_hour,
    callsign,
    origin_country

