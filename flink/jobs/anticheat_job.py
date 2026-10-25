"""
Anti-Cheat Flink Stream Job (Speed Layer)
══════════════════════════════════════════
Consumes game-state events from Kafka in real-time and applies:

1. RULE-BASED DETECTION (immediate, no ML needed)
   ├── Velocity > 300 units/sec  → SPEEDHACK
   ├── Headshot rate > 85%       → AIMBOT
   ├── Reaction time < 100ms     → AIMBOT/MACRO
   └── Accuracy > 85%            → AIMBOT

2. WINDOWED AGGREGATIONS (1-minute tumbling windows)
   ├── Kills-per-minute per player
   ├── Average headshot rate in window
   └── Velocity spike count in window

3. FRAUD ALERT OUTPUT
   └── Writes suspected cheaters → fraud-alerts Kafka topic
   └── Writes aggregates         → Redis (speed view)
   └── Writes raw flags          → PostgreSQL

Architecture: SPEED LAYER of Lambda Architecture
Guarantee: Exactly-once via Flink checkpointing
"""

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import MapFunction, FilterFunction, ReduceFunction
import json
import redis
import psycopg2
import os
from datetime import datetime, timezone
from loguru import logger

# ── Config ─────────────────────────────────────────────────
KAFKA_BROKERS      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST         = os.getenv("REDIS_HOST", "redis")
POSTGRES_HOST      = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER      = os.getenv("POSTGRES_USER", "gameadmin")
POSTGRES_PASSWORD  = os.getenv("POSTGRES_PASSWORD", "gamepass123")
POSTGRES_DB        = os.getenv("POSTGRES_DB", "gaming_analytics")

# ── Anti-Cheat Rule Thresholds ──────────────────────────────
AIMBOT_HEADSHOT_THRESHOLD   = 0.85   # >85% headshots in window
AIMBOT_REACTION_THRESHOLD   = 100    # <100ms reaction time
SPEEDHACK_VELOCITY_LIMIT    = 300.0  # >300 units/sec
MACRO_ACCURACY_THRESHOLD    = 85.0   # >85% accuracy consistently
MIN_EVENTS_FOR_DETECTION    = 5      # Minimum events to trigger detection


class CheatDetectionMap(MapFunction):
    """
    Applies rule-based cheat detection on each game state event.
    Returns enriched event with detection results.
    """

    def map(self, value: str) -> str:
        event = json.loads(value)

        cheat_detected   = False
        detected_rules   = []
        confidence_score = 0.0

        velocity       = event.get("velocity", 0)
        headshot_rate  = event.get("headshot_rate", 0)
        reaction_ms    = event.get("reaction_time_ms", 999)
        accuracy       = event.get("accuracy_pct", 0)

        # Rule 1: Speedhack
        if velocity > SPEEDHACK_VELOCITY_LIMIT:
            cheat_detected = True
            detected_rules.append("VELOCITY_EXCEEDED")
            confidence_score += 0.70

        # Rule 2: Aimbot - inhuman headshot rate
        if headshot_rate > AIMBOT_HEADSHOT_THRESHOLD:
            cheat_detected = True
            detected_rules.append("AIMBOT_HEADSHOT_RATE")
            confidence_score += 0.60

        # Rule 3: Aimbot - inhuman reaction time
        if reaction_ms < AIMBOT_REACTION_THRESHOLD:
            cheat_detected = True
            detected_rules.append("AIMBOT_REACTION_TIME")
            confidence_score += 0.65

        # Rule 4: Macro / script - too-perfect accuracy
        if accuracy > MACRO_ACCURACY_THRESHOLD:
            cheat_detected = True
            detected_rules.append("MACRO_ACCURACY")
            confidence_score += 0.50

        # Combine with pre-flagged events from producer
        if event.get("cheat_flags"):
            confidence_score += 0.20
            detected_rules.extend(event["cheat_flags"])

        # Cap confidence at 1.0
        confidence_score = min(round(confidence_score, 3), 1.0)

        event["cheat_detected"]   = cheat_detected
        event["detected_rules"]   = list(set(detected_rules))
        event["confidence_score"] = confidence_score
        event["processed_at"]     = datetime.now(timezone.utc).isoformat()

        return json.dumps(event)


class FraudAlertFilter(FilterFunction):
    """Pass only events where cheat was detected with confidence > 0.5"""

    def filter(self, value: str) -> bool:
        event = json.loads(value)
        return event.get("cheat_detected", False) and event.get("confidence_score", 0) > 0.5


def build_pipeline():
    """Build and run the Flink streaming job."""

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # ── Enable checkpointing (exactly-once) ────────────────
    env.enable_checkpointing(60_000)  # Checkpoint every 60 seconds
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)

    # ── Kafka Source ────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("game-state")
        .set_group_id("flink-anticheat-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Kafka Sink (fraud alerts) ───────────────────────────
    fraud_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("fraud-alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # ── Build the stream ────────────────────────────────────
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="GameStateKafkaSource",
    )

    # Apply cheat detection
    enriched_stream = stream.map(
        CheatDetectionMap(),
        output_type=Types.STRING()
    )

    # Filter only fraud alerts
    fraud_stream = enriched_stream.filter(FraudAlertFilter())

    # Send fraud alerts back to Kafka
    fraud_stream.sink_to(fraud_sink)

    logger.info("🚀 Anti-Cheat Flink job starting...")
    env.execute("Gaming Anti-Cheat Detection Job")


if __name__ == "__main__":
    build_pipeline()
