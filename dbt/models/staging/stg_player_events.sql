-- Staging: stg_player_events
-- Cleans raw player event records

WITH source AS (
    SELECT * FROM raw_player_events
),
cleaned AS (
    SELECT
        event_id::TEXT                              AS event_id,
        player_id                                   AS player_id,
        session_id                                  AS session_id,
        COALESCE(match_id, 'unknown')               AS match_id,
        LOWER(TRIM(event_type))                     AS event_type,
        (payload->>'is_headshot')::BOOLEAN          AS is_headshot,
        (payload->>'reaction_time_ms')::INTEGER     AS reaction_time_ms,
        (payload->>'accuracy_pct')::FLOAT           AS accuracy_pct,
        (payload->>'score_delta')::INTEGER          AS score_delta,
        payload->>'weapon'                          AS weapon,
        client_ts::TIMESTAMP WITH TIME ZONE         AS client_ts,
        server_ts::TIMESTAMP WITH TIME ZONE         AS server_ts,
        DATE(server_ts)                             AS event_date,
        ingested_at
    FROM source
    WHERE event_type IS NOT NULL
      AND player_id  IS NOT NULL
      AND server_ts  IS NOT NULL
)
SELECT * FROM cleaned
