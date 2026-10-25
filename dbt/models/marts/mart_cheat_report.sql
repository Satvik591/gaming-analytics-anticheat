-- Mart: mart_cheat_report
-- Final anti-cheat report combining Flink speed layer output
-- with batch cheat signals from game state analysis

WITH cheat_signals AS (
    SELECT
        player_id,
        SUM(velocity_violations)        AS total_velocity_violations,
        SUM(aimbot_flags)               AS total_aimbot_flags,
        SUM(reaction_flags)             AS total_reaction_flags,
        SUM(macro_flags)                AS total_macro_flags,
        AVG(daily_risk_score)           AS avg_risk_score,
        MAX(daily_risk_score)           AS peak_risk_score,
        SUM(total_ticks)                AS total_ticks_analyzed,
        COUNT(DISTINCT state_date)      AS days_active
    FROM {{ ref('int_player_cheat_signals') }}
    GROUP BY player_id
),

cheat_reports AS (
    SELECT * FROM serving_cheat_reports
),

final AS (
    SELECT
        cs.player_id,
        -- Cheat signal summary
        cs.total_velocity_violations,
        cs.total_aimbot_flags,
        cs.total_reaction_flags,
        cs.total_macro_flags,
        cs.total_ticks_analyzed,
        ROUND(cs.avg_risk_score::NUMERIC, 4)    AS avg_risk_score,
        ROUND(cs.peak_risk_score::NUMERIC, 4)   AS peak_risk_score,
        -- Formal alerts from Flink speed layer
        cr.cheat_type,
        cr.confidence_score,
        cr.detection_method,
        cr.triggered_rules,
        cr.status,
        cr.detected_at,
        -- Risk classification
        CASE
            WHEN cs.peak_risk_score >= 0.7  THEN 'HIGH'
            WHEN cs.peak_risk_score >= 0.4  THEN 'MEDIUM'
            WHEN cs.peak_risk_score >= 0.1  THEN 'LOW'
            ELSE 'CLEAN'
        END                                     AS risk_level,
        -- Primary cheat type inferred from signal counts
        CASE
            WHEN cs.total_velocity_violations > cs.total_aimbot_flags
             AND cs.total_velocity_violations > cs.total_reaction_flags
                THEN 'speedhack'
            WHEN cs.total_aimbot_flags > cs.total_macro_flags
                THEN 'aimbot'
            WHEN cs.total_macro_flags > 0
                THEN 'macro'
            ELSE 'unknown'
        END                                     AS inferred_cheat_type,
        CURRENT_TIMESTAMP                       AS report_generated_at
    FROM cheat_signals cs
    LEFT JOIN cheat_reports cr ON cs.player_id = cr.player_id
    WHERE cs.avg_risk_score > 0.05  -- Only include players with some signal
)

SELECT * FROM final
ORDER BY peak_risk_score DESC
