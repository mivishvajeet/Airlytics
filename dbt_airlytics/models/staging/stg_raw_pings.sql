{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_flights', 'raw_pings') }}
),

deduplicated AS (
    SELECT
        message_id,
        CAST(ingest_timestamp AS TIMESTAMP) AS ingest_timestamp,
        CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        CAST(event_time_epoch AS INT64) AS event_time_epoch,
        LOWER(TRIM(icao24)) AS icao24,
        NULLIF(UPPER(TRIM(callsign)), '') AS callsign,
        NULLIF(TRIM(origin_country), '') AS origin_country,
        CAST(longitude AS FLOAT64) AS longitude,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(baro_altitude AS FLOAT64) AS altitude_baro_m,
        COALESCE(on_ground, FALSE) AS on_ground,
        CAST(velocity AS FLOAT64) AS velocity_ms,
        CAST(true_track AS FLOAT64) AS heading_deg,
        CAST(vertical_rate AS FLOAT64) AS vertical_rate_ms,
        CAST(geo_altitude AS FLOAT64) AS altitude_geom_m,
        squawk,
        COALESCE(spi, FALSE) AS spi,
        position_source,
        ST_GEOGPOINT(CAST(longitude AS FLOAT64), CAST(latitude AS FLOAT64)) AS ping_point
    FROM source
    WHERE
        latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90.0 AND 90.0
        AND longitude BETWEEN -180.0 AND 180.0
        AND icao24 IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY icao24, event_timestamp 
        ORDER BY ingest_timestamp DESC
    ) = 1
)

SELECT * FROM deduplicated
