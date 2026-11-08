{{
    config(
        materialized='table',
        cluster_by=['ident', 'iata_code']
    )
}}

SELECT
    airport_id,
    ident,
    type,
    name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,
    scheduled_service,
    gps_code,
    iata_code,
    local_code,
    airport_point,
    geofence_polygon
FROM {{ ref('stg_airports') }}

