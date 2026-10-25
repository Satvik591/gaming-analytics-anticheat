"""
Game State Producer
────────────────────
Simulates real-time game state telemetry (server-side ticks):
player positions, velocities, health, headshot rates, reaction times.
This is the PRIMARY source for anti-cheat detection.

Cheating indicators injected at FRAUD_INJECTION_RATE:
  - Aimbot: near-perfect headshot rate (>90%), super low reaction time (<80ms)
  - Speedhack: velocity > physical game limit
  - Wallhack: perfect positioning relative to enemies (simulated via flags)

Topic: game-state
"""
import uuid
import time
import random
import json
import math
from datetime import datetime, timezone
from confluent_kafka import Producer
from loguru import logger

from config import KAFKA_CONFIG, TOPICS, NUM_PLAYERS, NUM_MATCHES, EVENTS_PER_SECOND, FRAUD_INJECTION_RATE

PLAYER_IDS = [f"player_{i:05d}" for i in range(NUM_PLAYERS)]
MATCH_IDS  = [f"match_{uuid.uuid4().hex[:8]}" for _ in range(NUM_MATCHES)]

MAX_LEGITIMATE_VELOCITY = 300.0   # units/sec — game engine limit
MAX_LEGITIMATE_REACTION = 120     # ms — human physiological minimum ≈ 120ms


def generate_game_state(tick: int, is_cheating: bool = False) -> dict:
    player_id = random.choice(PLAYER_IDS)
    match_id  = random.choice(MATCH_IDS)

    # Normal player stats
    velocity     = round(random.uniform(0, 280), 2)
    headshot_rt  = round(random.uniform(0.15, 0.40), 3)
    reaction_ms  = random.randint(150, 550)
    accuracy     = round(random.uniform(25, 65), 2)

    cheat_flags = []

    if is_cheating:
        cheat_type = random.choice(["aimbot", "speedhack", "wallhack", "macro"])

        if cheat_type == "aimbot":
            headshot_rt = round(random.uniform(0.88, 0.99), 3)
            reaction_ms = random.randint(10, 75)   # Inhuman reaction time
            accuracy    = round(random.uniform(87, 99), 2)
            cheat_flags.append("AIMBOT_SUSPECTED")

        elif cheat_type == "speedhack":
            velocity    = round(random.uniform(400, 900), 2)  # Beyond engine limit
            cheat_flags.append("SPEEDHACK_SUSPECTED")

        elif cheat_type == "wallhack":
            cheat_flags.append("WALLHACK_SUSPECTED")  # Flag for Flink to score

        elif cheat_type == "macro":
            reaction_ms = random.randint(85, 100)   # Too consistent
            accuracy    = round(random.uniform(78, 95), 2)
            cheat_flags.append("MACRO_SUSPECTED")

    return {
        "state_id":       str(uuid.uuid4()),
        "match_id":       match_id,
        "player_id":      player_id,
        "tick_number":    tick,
        "position": {
            "x": round(random.uniform(-500, 500), 3),
            "y": round(random.uniform(-500, 500), 3),
            "z": round(random.uniform(0, 50), 3),
        },
        "velocity":           velocity,
        "health":             random.randint(1, 100),
        "kills":              random.randint(0, 40),
        "headshot_rate":      headshot_rt,
        "reaction_time_ms":   reaction_ms,
        "accuracy_pct":       accuracy,
        "cheat_flags":        cheat_flags,
        "is_suspicious":      is_cheating,
        "exceeds_velocity_limit": velocity > MAX_LEGITIMATE_VELOCITY,
        "inhuman_reaction":   reaction_ms < MAX_LEGITIMATE_REACTION,
        "state_ts":           datetime.now(timezone.utc).isoformat(),
        "schema_version":     "1.0",
    }


def run_producer(events_per_second: int = EVENTS_PER_SECOND):
    producer = Producer(KAFKA_CONFIG)
    topic    = TOPICS["game_state"]
    interval = 1.0 / events_per_second

    logger.info(f"🎯 Game State Producer started | {events_per_second} ticks/sec → topic: {topic}")
    logger.info(f"   Cheat injection rate: {FRAUD_INJECTION_RATE * 100:.1f}%")

    tick = 0
    try:
        while True:
            start = time.monotonic()
            is_cheating = random.random() < FRAUD_INJECTION_RATE
            state = generate_game_state(tick=tick, is_cheating=is_cheating)

            producer.produce(
                topic=topic,
                key=state["player_id"],
                value=json.dumps(state).encode("utf-8"),
            )
            producer.poll(0)

            tick += 1
            if tick % 200 == 0:
                logger.info(f"🎯 Tick {tick} | suspicious={is_cheating} | flags={state['cheat_flags']}")

            elapsed    = time.monotonic() - start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info(f"Game state producer stopped at tick {tick}")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_producer()
