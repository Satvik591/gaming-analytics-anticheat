-- Intermediate: int_player_cheat_signals
-- Aggregates cheat indicators per player per day
-- Used by mart_cheat_report to build final risk profiles

WITH game_state_daily AS (
    SELECT
        player_id,
        state_date,
        COUNT(*)                                    AS total_ticks,
        AVG(velocity)                               AS avg_velocity,
        MAX(velocity)                               AS max_velocity,
        AVG(headshot_rate)                          AS avg_headshot_rate,
        AVG(reaction_time_ms)                       AS avg_reaction_ms,
        MIN(reaction_time_ms)                       AS min_reaction_ms,
        AVG(accuracy_pct)                           AS avg_accuracy,
        -- Cheat signal counts
        SUM(CASE WHEN velocity_exceeded  THEN 1 ELSE 0 END)  AS velocity_violations,
        SUM(CASE WHEN aimbot_headshot    THEN 1 ELSE 0 END)  AS aimbot_flags,
        SUM(CASE WHEN inhuman_reaction   THEN 1 ELSE 0 END)  AS reaction_flags,
        SUM(CASE WHEN macro_accuracy     THEN 1 ELSE 0 END)  AS macro_flags,
        -- Composite cheat signal: how many ticks had ANY flag
        SUM(CASE WHEN velocity_exceeded OR aimbot_headshot
                   OR inhuman_reaction OR macro_accuracy
                 THEN 1 ELSE 0 END)                          AS total_cheat_ticks
    FROM {{ ref('stg_game_state') }}
    GROUP BY player_id, state_date
),

with_rates AS (
    SELECT
        *,
        -- What % of this player's ticks were flagged?
        ROUND(
            (total_cheat_ticks::NUMERIC / NULLIF(total_ticks, 0)) * 100,
            2
        )                                           AS cheat_tick_pct,
        -- Composite risk score 0-1
        ROUND(LEAST(
            (velocity_violations * 0.3 +
             aimbot_flags        * 0.3 +
             reaction_flags      * 0.25 +
             macro_flags         * 0.15) / NULLIF(total_ticks, 0),
        1.0)::NUMERIC, 4)                           AS daily_risk_score
    FROM game_state_daily
)

SELECT * FROM with_rates
ORDER BY daily_risk_score DESC
