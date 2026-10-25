"""
Match Logs Producer
────────────────────
Simulates match lifecycle events: match_start, match_end,
round completions, and final stats. Publishes every ~30 seconds
(configurable) to simulate real match endings.

Topic: match-logs
"""
import uuid
import time
import random
import json
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from faker import Faker
from loguru import logger

from config import KAFKA_CONFIG, TOPICS, NUM_PLAYERS, NUM_MATCHES, MAPS, MATCH_MODES, REGIONS

fake = Faker()
PLAYER_IDS = [f"player_{i:05d}" for i in range(NUM_PLAYERS)]


def generate_match_log() -> dict:
    match_id       = f"match_{uuid.uuid4().hex[:8]}"
    num_players    = random.choice([2, 5, 10])
    players        = random.sample(PLAYER_IDS, num_players)
    duration_min   = round(random.uniform(8, 45), 1)
    start_ts       = datetime.now(timezone.utc) - timedelta(minutes=duration_min)

    team_a = players[:num_players // 2]
    team_b = players[num_players // 2:]
    winner = random.choice(["team_a", "team_b"])

    player_stats = {}
    for pid in players:
        player_stats[pid] = {
            "kills":        random.randint(0, 35),
            "deaths":       random.randint(0, 25),
            "assists":      random.randint(0, 15),
            "damage_dealt": random.randint(0, 8000),
            "headshots":    random.randint(0, 20),
            "mvp":          False,
        }

    # Pick MVP
    mvp = max(player_stats, key=lambda p: player_stats[p]["kills"])
    player_stats[mvp]["mvp"] = True

    return {
        "log_id":       str(uuid.uuid4()),
        "match_id":     match_id,
        "match_mode":   random.choice(MATCH_MODES),
        "map_name":     random.choice(MAPS),
        "region":       random.choice(REGIONS),
        "start_ts":     start_ts.isoformat(),
        "end_ts":       datetime.now(timezone.utc).isoformat(),
        "duration_min": duration_min,
        "players":      players,
        "team_a":       team_a,
        "team_b":       team_b,
        "winner":       winner,
        "player_stats": player_stats,
        "schema_version": "1.0",
    }


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Match log delivered → offset {msg.offset()}")


def run_producer(interval_seconds: int = 30):
    producer = Producer(KAFKA_CONFIG)
    topic    = TOPICS["match_logs"]

    logger.info(f"🏆 Match Logs Producer started | 1 event every {interval_seconds}s → topic: {topic}")

    total_sent = 0
    try:
        while True:
            log = generate_match_log()
            producer.produce(
                topic=topic,
                key=log["match_id"],
                value=json.dumps(log).encode("utf-8"),
                callback=delivery_report,
            )
            producer.flush()
            total_sent += 1
            logger.info(f"🏆 Match log sent | match_id={log['match_id']} mode={log['match_mode']} map={log['map_name']}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info(f"Producer stopped. Total match logs sent: {total_sent}")


if __name__ == "__main__":
    run_producer()
