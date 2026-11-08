{{
    config(
        materialized='table',
        partition_by={
            "field": "event_timestamp",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['event_type', 'airport_ident']
    )
}}

WITH pings AS (
    SELECT * FROM {{ ref('stg_raw_pings') }}
    WHERE vertical_rate_ms IS NOT NULL AND ABS(vertical_rate_ms) >= 1.5
),

airports AS (
    SELECT * FROM {{ ref('dim_airport') }}
),

spatial_matches AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(p.message_id, '_', a.ident)) AS event_id,
        p.message_id AS ping_id,
        a.ident AS airport_ident,
        a.name AS airport_name,
        a.iso_country,
        CASE
            WHEN p.vertical_rate_ms >= 1.5 THEN 'TAKEOFF'
            ELSE 'LANDING'
        END AS event_type,
        p.icao24,
        p.callsign,
        p.event_timestamp,
        p.latitude,
        p.longitude,
        p.altitude_baro_m,
        p.velocity_ms,
        p.vertical_rate_ms,
        ST_DISTANCE(p.ping_point, a.airport_point) AS distance_to_airport_m
    FROM pings p
    JOIN airports a
        ON ST_CONTAINS(a.geofence_polygon, p.ping_point)
)

SELECT * FROM spatial_matches

