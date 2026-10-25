-- Intermediate: int_player_session_stats
-- Per-player per-session aggregated stats
-- Feeds into mart_leaderboard and mart_player_profile

WITH events AS (
    SELECT * FROM {{ ref('stg_player_events') }}
),

session_agg AS (
    SELECT
        player_id,
        session_id,
        match_id,
        event_date,
        MIN(client_ts)                                      AS session_start,
        MAX(client_ts)                                      AS session_end,
        EXTRACT(EPOCH FROM (MAX(client_ts) - MIN(client_ts))) / 60
                                                            AS session_duration_min,
        COUNT(*)                                            AS total_events,
        COUNT(CASE WHEN event_type = 'kill'   THEN 1 END)  AS kills,
        COUNT(CASE WHEN event_type = 'death'  THEN 1 END)  AS deaths,
        COUNT(CASE WHEN event_type = 'kill'
                    AND is_headshot = TRUE    THEN 1 END)  AS headshot_kills,
        AVG(CASE WHEN reaction_time_ms > 0
                 THEN reaction_time_ms END)                 AS avg_reaction_ms,
        AVG(accuracy_pct)                                   AS avg_accuracy
    FROM events
    GROUP BY player_id, session_id, match_id, event_date
),

with_ratios AS (
    SELECT
        *,
        ROUND(kills::NUMERIC / GREATEST(deaths, 1), 3)     AS kd_ratio,
        ROUND(
            headshot_kills::NUMERIC / GREATEST(kills, 1), 3
        )                                                   AS headshot_rate
    FROM session_agg
)

SELECT * FROM with_ratios
