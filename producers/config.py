"""
Shared configuration and utilities for all producers.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Kafka Config ────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "client.id": "gaming-producer",
    "acks": "all",
    "retries": 3,
    "linger.ms": 5,
    "batch.size": 16384,
}

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# ── Kafka Topics ────────────────────────────────────────────
TOPICS = {
    "player_events": "player-events",
    "match_logs":    "match-logs",
    "chat":          "chat-messages",
    "purchases":     "purchases",
    "game_state":    "game-state",
    "fraud_alerts":  "fraud-alerts",
}

# ── Simulation Config ───────────────────────────────────────
EVENTS_PER_SECOND    = int(os.getenv("EVENTS_PER_SECOND", 15))
FRAUD_INJECTION_RATE = float(os.getenv("FRAUD_INJECTION_RATE", 0.05))
NUM_PLAYERS          = int(os.getenv("NUM_PLAYERS", 1000))
NUM_MATCHES          = int(os.getenv("NUM_MATCHES", 50))

# ── Game Constants ──────────────────────────────────────────
MAPS        = ["Dust2", "Inferno", "Mirage", "Nuke", "Overpass", "Vertigo", "Anubis"]
MATCH_MODES = ["ranked", "casual", "tournament", "deathmatch"]
REGIONS     = ["NA", "EU", "APAC", "SA", "OCE", "ME"]
ITEM_TYPES  = ["skin", "weapon", "battlepass", "currency", "emote", "spray"]
CHAT_CHANNELS = ["team", "global", "whisper"]
EVENT_TYPES   = ["kill", "death", "move", "ability_use", "disconnect", "connect",
                  "match_start", "match_end", "respawn", "item_pickup"]
