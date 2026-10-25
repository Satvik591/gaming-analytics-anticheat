"""
Player Events Producer
──────────────────────
Simulates real-time player actions: kills, deaths, movements,
ability usage, connects/disconnects at ≥10 events/sec.

Data Contract (Avro): kafka/schemas/player_event.avsc
Topic: player-events
"""
import uuid
import time
import random
import json
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker
from loguru import logger

from config import KAFKA_CONFIG, TOPICS, EVENTS_PER_SECOND, NUM_PLAYERS, NUM_MATCHES, EVENT_TYPES, FRAUD_INJECTION_RATE

fake = Faker()

# Pre-generate stable player and match IDs for realism
PLAYER_IDS = [f"player_{i:05d}" for i in range(NUM_PLAYERS)]
MATCH_IDS  = [f"match_{uuid.uuid4().hex[:8]}" for _ in range(NUM_MATCHES)]


def generate_player_event(is_cheating: bool = False) -> dict:
    """Generate a single player event. If is_cheating=True, inject suspicious stats."""
    player_id  = random.choice(PLAYER_IDS)
    match_id   = random.choice(MATCH_IDS)
    event_type = random.choice(EVENT_TYPES)

    payload = {
        "score_delta": random.randint(-10, 50),
        "position": {
            "x": round(random.uniform(-500, 500), 2),
            "y": round(random.uniform(-500, 500), 2),
            "z": round(random.uniform(0, 100), 2),
        },
        "weapon": random.choice(["AK47", "M4A1", "AWP", "Pistol", "Knife", None]),
        "is_headshot": random.random() < (0.95 if is_cheating else 0.25),
        "reaction_time_ms": random.randint(5, 80) if is_cheating else random.randint(150, 600),
        "accuracy_pct": round(random.uniform(85, 99) if is_cheating else random.uniform(20, 65), 2),
    }

    return {
        "event_id":   str(uuid.uuid4()),
        "player_id":  player_id,
        "session_id": f"session_{uuid.uuid4().hex[:12]}",
        "match_id":   match_id,
        "event_type": event_type,
        "payload":    payload,
        "is_suspicious": is_cheating,
        "client_ts":  datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for event {msg.key()}: {err}")
    else:
        logger.debug(f"Event delivered → {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")


def run_producer(events_per_second: int = EVENTS_PER_SECOND):
    producer = Producer(KAFKA_CONFIG)
    topic    = TOPICS["player_events"]
    interval = 1.0 / events_per_second

    logger.info(f"🎮 Player Events Producer started | {events_per_second} events/sec → topic: {topic}")

    total_sent = 0
    try:
        while True:
            start = time.monotonic()

            # Inject fraud at configured rate
            is_cheating = random.random() < FRAUD_INJECTION_RATE
            event = generate_player_event(is_cheating=is_cheating)

            producer.produce(
                topic=topic,
                key=event["player_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)  # non-blocking poll

            total_sent += 1
            if total_sent % 100 == 0:
                logger.info(f"📊 Sent {total_sent} player events | Cheating injected: {is_cheating}")

            elapsed = time.monotonic() - start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.flush()
        logger.info(f"✅ Total events sent: {total_sent}")


if __name__ == "__main__":
    run_producer()
