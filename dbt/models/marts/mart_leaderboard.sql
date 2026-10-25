-- Mart: mart_leaderboard
-- Final leaderboard table — the BATCH VIEW of Lambda Architecture
-- Refreshed hourly by Spark + then enriched by dbt

WITH session_stats AS (
    SELECT * FROM {{ ref('int_player_session_stats') }}
),

player_totals AS (
    SELECT
        player_id,
        SUM(kills)                                      AS total_kills,
        SUM(deaths)                                     AS total_deaths,
        SUM(headshot_kills)                             AS total_headshots,
        COUNT(DISTINCT session_id)                      AS total_sessions,
        COUNT(DISTINCT match_id)                        AS matches_played,
        SUM(session_duration_min)                       AS total_playtime_min,
        AVG(avg_reaction_ms)                            AS avg_reaction_ms,
        AVG(avg_accuracy)                               AS avg_accuracy_pct,
        MAX(session_end)                                AS last_active
    FROM session_stats
    GROUP BY player_id
),

with_ratios AS (
    SELECT
        *,
        ROUND(total_kills::NUMERIC / GREATEST(total_deaths, 1), 3) AS kd_ratio,
        ROUND(total_headshots::NUMERIC / GREATEST(total_kills, 1), 3) AS overall_headshot_rate,
        ROUND(total_playtime_min / 60.0, 2)             AS total_playtime_hrs
    FROM player_totals
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            ORDER BY kd_ratio DESC, total_kills DESC, matches_played DESC
        )                                               AS rank,
        CURRENT_TIMESTAMP                               AS batch_computed_at
    FROM with_ratios
)

SELECT
    player_id,
    rank,
    total_kills,
    total_deaths,
    total_headshots,
    kd_ratio,
    overall_headshot_rate,
    matches_played,
    total_sessions,
    total_playtime_hrs,
    ROUND(avg_reaction_ms::NUMERIC, 0)                  AS avg_reaction_ms,
    ROUND(avg_accuracy_pct::NUMERIC, 2)                 AS avg_accuracy_pct,
    last_active,
    batch_computed_at
FROM ranked
ORDER BY rank ASC
