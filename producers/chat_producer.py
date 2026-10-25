"""
Chat Messages Producer
───────────────────────
Simulates in-game chat: team chat, global chat, whispers.
Randomly injects toxic/suspicious messages for NLP flagging.

Topic: chat-messages
"""
import uuid
import time
import random
import json
from datetime import datetime, timezone
from confluent_kafka import Producer
from loguru import logger

from config import KAFKA_CONFIG, TOPICS, NUM_PLAYERS, CHAT_CHANNELS, EVENTS_PER_SECOND

PLAYER_IDS = [f"player_{i:05d}" for i in range(NUM_PLAYERS)]

NORMAL_MESSAGES = [
    "GG!", "Nice shot!", "Let's go team!", "Cover me!", "Rotating now",
    "Need backup at B site", "Enemy spotted north", "I'm low, need heal",
    "Good game everyone", "Let's push together", "Watch out behind you!",
]

SUSPICIOUS_MESSAGES = [
    "I know exactly where you are lol",
    "Can see through walls ez",
    "My aim is perfect today ;)",
    "Never miss a headshot",
    "Using a little help hehe",
]

TOXIC_MESSAGES = [
    "You're all terrible players",
    "This team is hopeless",
    "Report this cheater!!!",
    "HACKER ALERT!!!",
]


def generate_chat_message() -> dict:
    category = random.choices(
        ["normal", "suspicious", "toxic"],
        weights=[0.80, 0.05, 0.15]
    )[0]

    if category == "normal":
        text = random.choice(NORMAL_MESSAGES)
    elif category == "suspicious":
        text = random.choice(SUSPICIOUS_MESSAGES)
    else:
        text = random.choice(TOXIC_MESSAGES)

    return {
        "message_id":     str(uuid.uuid4()),
        "player_id":      random.choice(PLAYER_IDS),
        "match_id":       f"match_{uuid.uuid4().hex[:8]}",
        "channel":        random.choice(CHAT_CHANNELS),
        "message_text":   text,
        "category":       category,
        "flagged":        category in ["suspicious", "toxic"],
        "client_ts":      datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


def run_producer(events_per_second: int = max(1, EVENTS_PER_SECOND // 3)):
    producer = Producer(KAFKA_CONFIG)
    topic    = TOPICS["chat"]
    interval = 1.0 / events_per_second

    logger.info(f"💬 Chat Producer started | {events_per_second} msgs/sec → topic: {topic}")

    total_sent = 0
    try:
        while True:
            msg = generate_chat_message()
            producer.produce(
                topic=topic,
                key=msg["player_id"],
                value=json.dumps(msg).encode("utf-8"),
            )
            producer.poll(0)
            total_sent += 1
            if total_sent % 50 == 0:
                logger.info(f"💬 Sent {total_sent} chat messages")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info(f"Chat producer stopped. Sent: {total_sent}")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_producer()
