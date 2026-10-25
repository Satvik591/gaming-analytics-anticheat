-- Staging: stg_purchases
-- Cleans raw purchase records

WITH source AS (
    SELECT * FROM raw_purchases
),
cleaned AS (
    SELECT
        purchase_id::TEXT                           AS purchase_id,
        player_id                                   AS player_id,
        item_id                                     AS item_id,
        LOWER(TRIM(item_type))                      AS item_type,
        ROUND(amount_usd::NUMERIC, 2)               AS amount_usd,
        UPPER(currency)                             AS currency,
        LOWER(status)                               AS status,
        purchase_ts::TIMESTAMP WITH TIME ZONE       AS purchase_ts,
        DATE(purchase_ts)                           AS purchase_date,
        -- Flag high-value single purchases
        CASE WHEN amount_usd > 50 THEN TRUE ELSE FALSE END AS is_high_value,
        ingested_at
    FROM source
    WHERE player_id   IS NOT NULL
      AND amount_usd  > 0
      AND status      = 'completed'
)
SELECT * FROM cleaned
