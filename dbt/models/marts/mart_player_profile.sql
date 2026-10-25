-- Mart: mart_player_profile
-- Complete player profile combining activity, purchases, and risk data

WITH session_stats AS (
    SELECT
        player_id,
        COUNT(DISTINCT match_id)        AS matches_played,
        COUNT(DISTINCT session_id)      AS total_sessions,
        SUM(session_duration_min)       AS total_playtime_min,
        AVG(kd_ratio)                   AS avg_kd_ratio,
        MAX(session_end)                AS last_active,
        MIN(session_start)              AS first_seen,
        -- Favourite match mode proxy via most common match
        COUNT(DISTINCT match_id)        AS unique_matches
    FROM {{ ref('int_player_session_stats') }}
    GROUP BY player_id
),

purchase_stats AS (
    SELECT
        player_id,
        COUNT(*)                            AS total_purchases,
        ROUND(SUM(amount_usd)::NUMERIC, 2)  AS total_spent_usd,
        ROUND(AVG(amount_usd)::NUMERIC, 2)  AS avg_purchase_usd,
        MAX(amount_usd)                     AS max_single_purchase,
        COUNT(CASE WHEN is_high_value THEN 1 END) AS high_value_purchases
    FROM {{ ref('stg_purchases') }}
    GROUP BY player_id
),

risk_data AS (
    SELECT
        player_id,
        avg_risk_score,
        peak_risk_score,
        total_aimbot_flags,
        total_velocity_violations,
        total_reaction_flags
    FROM {{ ref('mart_cheat_report') }}
),

final AS (
    SELECT
        ss.player_id,
        -- Activity metrics
        ss.matches_played,
        ss.total_sessions,
        ROUND(ss.total_playtime_min / 60.0, 2)  AS total_playtime_hrs,
        ROUND(ss.avg_kd_ratio::NUMERIC, 3)       AS avg_kd_ratio,
        ss.first_seen,
        ss.last_active,
        -- Purchase metrics
        COALESCE(ps.total_purchases, 0)          AS total_purchases,
        COALESCE(ps.total_spent_usd, 0)          AS total_spent_usd,
        COALESCE(ps.avg_purchase_usd, 0)         AS avg_purchase_usd,
        COALESCE(ps.high_value_purchases, 0)     AS high_value_purchases,
        -- Risk metrics
        COALESCE(rd.avg_risk_score, 0)           AS avg_risk_score,
        COALESCE(rd.peak_risk_score, 0)          AS peak_risk_score,
        CASE
            WHEN COALESCE(rd.peak_risk_score, 0) >= 0.7 THEN 'HIGH_RISK'
            WHEN COALESCE(rd.peak_risk_score, 0) >= 0.4 THEN 'MEDIUM_RISK'
            WHEN COALESCE(rd.peak_risk_score, 0) >= 0.1 THEN 'LOW_RISK'
            ELSE 'CLEAN'
        END                                      AS player_risk_category,
        CURRENT_TIMESTAMP                        AS profile_updated_at
    FROM session_stats ss
    LEFT JOIN purchase_stats ps ON ss.player_id = ps.player_id
    LEFT JOIN risk_data rd      ON ss.player_id = rd.player_id
)

SELECT * FROM final
ORDER BY peak_risk_score DESC NULLS LAST
