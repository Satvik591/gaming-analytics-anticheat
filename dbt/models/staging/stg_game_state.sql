-- Staging: stg_game_state
-- Cleans and type-casts raw game state telemetry
-- Adds derived cheat indicator columns

WITH source AS (
    SELECT * FROM raw_game_state
),
cleaned AS (
    SELECT
        state_id::TEXT                  AS state_id,
        match_id                        AS match_id,
        player_id                       AS player_id,
        tick_number                     AS tick_number,
        position_x                      AS pos_x,
        position_y                      AS pos_y,
        position_z                      AS pos_z,
        ROUND(velocity::NUMERIC, 2)     AS velocity,
        GREATEST(health, 0)             AS health,
        GREATEST(kills, 0)              AS kills,
        ROUND(headshot_rate::NUMERIC, 4)    AS headshot_rate,
        GREATEST(reaction_time_ms, 0)       AS reaction_time_ms,
        ROUND(accuracy_pct::NUMERIC, 2)     AS accuracy_pct,
        -- Derived cheat indicators (used downstream)
        CASE WHEN velocity > 300           THEN TRUE ELSE FALSE END AS velocity_exceeded,
        CASE WHEN headshot_rate > 0.85     THEN TRUE ELSE FALSE END AS aimbot_headshot,
        CASE WHEN reaction_time_ms < 100   THEN TRUE ELSE FALSE END AS inhuman_reaction,
        CASE WHEN accuracy_pct > 85        THEN TRUE ELSE FALSE END AS macro_accuracy,
        state_ts::TIMESTAMP WITH TIME ZONE AS state_ts,
        DATE(state_ts)                      AS state_date,
        ingested_at
    FROM source
    WHERE player_id IS NOT NULL
      AND match_id  IS NOT NULL
      AND state_ts  IS NOT NULL
)
SELECT * FROM cleaned
