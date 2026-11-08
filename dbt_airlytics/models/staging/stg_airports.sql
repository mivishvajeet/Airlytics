{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_flights', 'raw_airports') }}
)

SELECT
    CAST(id AS INT64) AS airport_id,
    UPPER(TRIM(ident)) AS ident,
    TRIM(type) AS type,
    TRIM(name) AS name,
    CAST(latitude_deg AS FLOAT64) AS latitude_deg,
    CAST(longitude_deg AS FLOAT64) AS longitude_deg,
    CAST(elevation_ft AS INT64) AS elevation_ft,
    TRIM(continent) AS continent,
    TRIM(iso_country) AS iso_country,
    TRIM(iso_region) AS iso_region,
    TRIM(municipality) AS municipality,
    TRIM(scheduled_service) AS scheduled_service,
    TRIM(gps_code) AS gps_code,
    NULLIF(TRIM(iata_code), '') AS iata_code,
    TRIM(local_code) AS local_code,
    ST_GEOGPOINT(CAST(longitude_deg AS FLOAT64), CAST(latitude_deg AS FLOAT64)) AS airport_point,
    ST_BUFFER(ST_GEOGPOINT(CAST(longitude_deg AS FLOAT64), CAST(latitude_deg AS FLOAT64)), 5000.0) AS geofence_polygon
FROM source
WHERE
    latitude_deg IS NOT NULL
    AND longitude_deg IS NOT NULL
    AND CAST(latitude_deg AS FLOAT64) BETWEEN -90.0 AND 90.0
    AND CAST(longitude_deg AS FLOAT64) BETWEEN -180.0 AND 180.0
