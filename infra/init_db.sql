-- ═══════════════════════════════════════════════════════════
-- Gaming Analytics & Anti-Cheat System
-- Database Initialization Script
-- ═══════════════════════════════════════════════════════════

-- ── RAW LAYER (Speed layer writes here) ──────────────────
CREATE TABLE IF NOT EXISTS raw_player_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       VARCHAR(50) NOT NULL,
    session_id      VARCHAR(50) NOT NULL,
    match_id        VARCHAR(50),
    event_type      VARCHAR(50) NOT NULL,  -- kill, death, move, ability_use, disconnect
    payload         JSONB,
    client_ts       TIMESTAMP NOT NULL,
    server_ts       TIMESTAMP DEFAULT NOW(),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_match_logs (
    log_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_id        VARCHAR(50) NOT NULL,
    match_mode      VARCHAR(30),           -- ranked, casual, tournament
    map_name        VARCHAR(50),
    start_ts        TIMESTAMP,
    end_ts          TIMESTAMP,
    winner_team     VARCHAR(10),
    players         JSONB,                 -- array of player_ids
    stats           JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_chat_messages (
    message_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       VARCHAR(50) NOT NULL,
    match_id        VARCHAR(50),
    channel         VARCHAR(20),           -- team, global, whisper
    message_text    TEXT,
    sentiment_score FLOAT,
    flagged         BOOLEAN DEFAULT FALSE,
    client_ts       TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_purchases (
    purchase_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       VARCHAR(50) NOT NULL,
    item_id         VARCHAR(50),
    item_type       VARCHAR(30),           -- skin, weapon, battlepass, currency
    amount_usd      DECIMAL(10,2),
    currency        VARCHAR(10) DEFAULT 'USD',
    status          VARCHAR(20) DEFAULT 'completed',
    purchase_ts     TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_game_state (
    state_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    match_id        VARCHAR(50) NOT NULL,
    player_id       VARCHAR(50) NOT NULL,
    tick_number     BIGINT,
    position_x      FLOAT,
    position_y      FLOAT,
    position_z      FLOAT,
    velocity        FLOAT,
    health          INTEGER,
    kills           INTEGER DEFAULT 0,
    headshot_rate   FLOAT DEFAULT 0.0,
    reaction_time_ms INTEGER,
    state_ts        TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ── SERVING LAYER (Batch views from Spark+dbt) ───────────
CREATE TABLE IF NOT EXISTS serving_leaderboard (
    player_id       VARCHAR(50) PRIMARY KEY,
    username        VARCHAR(100),
    rank            INTEGER,
    total_kills     BIGINT DEFAULT 0,
    total_deaths    BIGINT DEFAULT 0,
    kd_ratio        FLOAT,
    win_rate        FLOAT,
    matches_played  INTEGER DEFAULT 0,
    total_playtime_hrs FLOAT DEFAULT 0,
    last_updated    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS serving_cheat_reports (
    report_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id       VARCHAR(50) NOT NULL,
    match_id        VARCHAR(50),
    cheat_type      VARCHAR(50),           -- aimbot, wallhack, speedhack, macro
    confidence_score FLOAT,               -- ML model confidence 0-1
    detection_method VARCHAR(30),         -- rule_based, ml_model, both
    triggered_rules  TEXT[],
    evidence_payload JSONB,
    status          VARCHAR(20) DEFAULT 'pending',  -- pending, confirmed, dismissed
    detected_at     TIMESTAMP DEFAULT NOW(),
    reviewed_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS serving_player_profiles (
    player_id       VARCHAR(50) PRIMARY KEY,
    username        VARCHAR(100),
    region          VARCHAR(20),
    account_created TIMESTAMP,
    total_purchases_usd DECIMAL(10,2) DEFAULT 0,
    avg_session_duration_min FLOAT,
    favourite_mode  VARCHAR(30),
    risk_score      FLOAT DEFAULT 0.0,    -- 0=clean, 1=high risk
    is_banned       BOOLEAN DEFAULT FALSE,
    ban_reason      TEXT,
    last_active     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS serving_match_summary (
    match_id        VARCHAR(50) PRIMARY KEY,
    match_mode      VARCHAR(30),
    map_name        VARCHAR(50),
    duration_min    FLOAT,
    total_players   INTEGER,
    total_kills     INTEGER,
    avg_kd          FLOAT,
    cheats_detected INTEGER DEFAULT 0,
    match_date      DATE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ── INDEXES ──────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_player_events_player_id ON raw_player_events(player_id);
CREATE INDEX IF NOT EXISTS idx_player_events_match_id  ON raw_player_events(match_id);
CREATE INDEX IF NOT EXISTS idx_player_events_server_ts ON raw_player_events(server_ts);
CREATE INDEX IF NOT EXISTS idx_game_state_match_player  ON raw_game_state(match_id, player_id);
CREATE INDEX IF NOT EXISTS idx_cheat_reports_player_id  ON serving_cheat_reports(player_id);
CREATE INDEX IF NOT EXISTS idx_cheat_reports_detected_at ON serving_cheat_reports(detected_at);

-- ── VIEWS ────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_top_100_players AS
SELECT player_id, username, rank, kd_ratio, win_rate, matches_played
FROM serving_leaderboard
ORDER BY rank ASC
LIMIT 100;

CREATE OR REPLACE VIEW vw_recent_cheaters AS
SELECT cr.player_id, pp.username, cr.cheat_type, cr.confidence_score,
       cr.detection_method, cr.detected_at, cr.status
FROM serving_cheat_reports cr
LEFT JOIN serving_player_profiles pp ON cr.player_id = pp.player_id
WHERE cr.detected_at > NOW() - INTERVAL '24 hours'
ORDER BY cr.confidence_score DESC;

RAISE NOTICE 'Database initialized successfully!';
