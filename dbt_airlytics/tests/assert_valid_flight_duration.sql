-- Fails if any flight leg has a negative duration or duration > 24 hours (1440 minutes)
SELECT
    flight_leg_id,
    icao24,
    callsign,
    session_start,
    session_end,
    duration_minutes
FROM {{ ref('fact_flight_leg') }}
WHERE
    duration_minutes < 0
    OR duration_minutes > 1440
