"""
Purchases Producer
───────────────────
Simulates in-game purchases: skins, weapons, battle passes,
in-game currency. Detects suspicious bulk-buy patterns.

Topic: purchases
"""
import uuid
import time
import random
import json
from datetime import datetime, timezone
from confluent_kafka import Producer
from loguru import logger

from config import KAFKA_CONFIG, TOPICS, NUM_PLAYERS, ITEM_TYPES

PLAYER_IDS = [f"player_{i:05d}" for i in range(NUM_PLAYERS)]

ITEMS = {
    "skin":        [("Dragon Lore",  45.99), ("Howl", 89.99), ("Fade", 12.99), ("Asiimov", 8.99)],
    "weapon":      [("M4A1-S", 15.99), ("AK47", 14.99), ("AWP", 19.99)],
    "battlepass":  [("Season Pass", 9.99), ("Premium Pass", 24.99)],
    "currency":    [("500 Coins", 4.99), ("1200 Coins", 9.99), ("2800 Coins", 19.99)],
    "emote":       [("Victory Dance", 2.99), ("Salute", 1.99)],
}


def generate_purchase(is_suspicious: bool = False) -> dict:
    player_id  = random.choice(PLAYER_IDS)
    item_type  = random.choice(ITEM_TYPES)
    items_list = ITEMS.get(item_type, [("Generic Item", 4.99)])
    item_name, base_price = random.choice(items_list)

    # Suspicious: multiple high-value purchases in quick succession
    quantity = random.randint(5, 20) if is_suspicious else 1
    amount   = round(base_price * quantity, 2)

    return {
        "purchase_id":    str(uuid.uuid4()),
        "player_id":      player_id,
        "item_id":        f"item_{uuid.uuid4().hex[:8]}",
        "item_name":      item_name,
        "item_type":      item_type,
        "quantity":       quantity,
        "amount_usd":     amount,
        "currency":       "USD",
        "status":         "completed",
        "is_suspicious":  is_suspicious,
        "purchase_ts":    datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


def run_producer(interval_seconds: float = 3.0):
    producer = Producer(KAFKA_CONFIG)
    topic    = TOPICS["purchases"]

    logger.info(f"💰 Purchases Producer started | 1 purchase every {interval_seconds}s → topic: {topic}")

    total_sent = 0
    try:
        while True:
            is_suspicious = random.random() < 0.03
            purchase = generate_purchase(is_suspicious=is_suspicious)
            producer.produce(
                topic=topic,
                key=purchase["player_id"],
                value=json.dumps(purchase).encode("utf-8"),
            )
            producer.flush()
            total_sent += 1
            logger.info(f"💰 Purchase: {purchase['item_name']} | \${purchase['amount_usd']} | suspicious={is_suspicious}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info(f"Purchases producer stopped. Sent: {total_sent}")


if __name__ == "__main__":
    run_producer()
