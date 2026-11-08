-- Fails if duplicate airport events exceed 0.1% of total landed events
WITH counts AS (
    SELECT
        COUNT(*) AS total_count,
        COUNT(DISTINCT event_id) AS distinct_count
    FROM {{ ref('fact_airport_event') }}
),

metrics AS (
    SELECT
        total_count,
        distinct_count,
        (total_count - distinct_count) AS duplicate_count,
        CASE
            WHEN total_count = 0 THEN 0.0
            ELSE (total_count - distinct_count) / CAST(total_count AS FLOAT64)
        END AS duplicate_rate
    FROM counts
)

SELECT *
FROM metrics
WHERE duplicate_rate > 0.001 -- > 0.1% SLA threshold

