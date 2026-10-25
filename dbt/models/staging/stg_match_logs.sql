-- Staging: stg_match_logs
-- Cleans raw match log records

WITH source AS (
    SELECT * FROM raw_match_logs
),
cleaned AS (
    SELECT
        log_id::TEXT                                AS log_id,
        match_id                                    AS match_id,
        LOWER(TRIM(match_mode))                     AS match_mode,
        map_name                                    AS map_name,
        region                                      AS region,
        start_ts::TIMESTAMP WITH TIME ZONE          AS start_ts,
        end_ts::TIMESTAMP WITH TIME ZONE            AS end_ts,
        ROUND(duration_min::NUMERIC, 2)             AS duration_min,
        players                                     AS players,
        winner                                      AS winner,
        stats                                       AS stats,
        DATE(end_ts)                                AS match_date,
        ingested_at
    FROM source
    WHERE match_id IS NOT NULL
      AND end_ts   IS NOT NULL
      AND duration_min > 0
)
SELECT * FROM cleaned
