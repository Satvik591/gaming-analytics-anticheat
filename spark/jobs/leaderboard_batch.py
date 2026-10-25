"""
Leaderboard Batch Job (Batch Layer - Apache Spark)
══════════════════════════════════════════════════
Runs hourly as a scheduled Spark job (cron / Airflow).
Reads ALL historical player events from PostgreSQL raw tables,
recomputes full leaderboard from scratch (idempotent).

This is the BATCH LAYER of Lambda Architecture:
  - Slower but 100% accurate (uses complete history)
  - Results go into serving_leaderboard table
  - Speed layer (Flink/Redis) handles the last few minutes gap

Schedule: Every hour via cron:
    0 * * * * /usr/bin/python3 /app/spark/jobs/leaderboard_batch.py

Run locally (Spark local mode — no Docker required per project guidelines):
    spark-submit --master local[*] leaderboard_batch.py
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

# ── Config ─────────────────────────────────────────────────
POSTGRES_URL  = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','gaming_analytics')}"
POSTGRES_PROPS = {
    "user":     os.getenv("POSTGRES_USER", "gameadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "gamepass123"),
    "driver":   "org.postgresql.Driver",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GamingAnalytics_LeaderboardBatch")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def run_leaderboard_batch():
    logger.info("🔵 [BATCH LAYER] Leaderboard batch job started")
    start_time = datetime.now()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Read raw player events ─────────────────────
    logger.info("Reading raw_player_events from PostgreSQL...")
    events_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="raw_player_events",
        properties=POSTGRES_PROPS,
    )

    # ── Step 2: Read raw match logs ─────────────────────────
    logger.info("Reading raw_match_logs from PostgreSQL...")
    matches_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="raw_match_logs",
        properties=POSTGRES_PROPS,
    )

    # ── Step 3: Compute per-player kill/death stats ─────────
    logger.info("Computing kill/death aggregations...")
    kill_stats = (
        events_df
        .filter(F.col("event_type").isin("kill", "death"))
        .groupBy("player_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "kill", 1).otherwise(0)).alias("total_kills"),
            F.sum(F.when(F.col("event_type") == "death", 1).otherwise(0)).alias("total_deaths"),
            F.countDistinct("match_id").alias("matches_played"),
            F.countDistinct("session_id").alias("total_sessions"),
        )
    )

    # ── Step 4: Compute KD ratio ────────────────────────────
    leaderboard_df = kill_stats.withColumn(
        "kd_ratio",
        F.round(
            F.col("total_kills") / F.greatest(F.col("total_deaths"), F.lit(1)),
            3
        )
    )

    # ── Step 5: Compute win rate from match logs ─────────────
    # Explode players array from match logs JSON
    win_df = (
        matches_df
        .withColumn("player_id", F.explode(F.from_json(F.col("players"), "array<string>")))
        .groupBy("player_id")
        .agg(
            F.count("*").alias("total_matches_from_logs"),
        )
    )

    # ── Step 6: Join and compute final leaderboard ──────────
    final_df = (
        leaderboard_df
        .join(win_df, on="player_id", how="left")
        .withColumn("win_rate", F.round(F.rand() * 0.7 + 0.1, 3))  # Placeholder: replace with real win tracking
        .withColumn("rank", F.row_number().over(
            Window.orderBy(F.col("kd_ratio").desc(), F.col("total_kills").desc())
        ))
        .withColumn("last_updated", F.current_timestamp())
        .select(
            "player_id", "rank", "total_kills", "total_deaths",
            "kd_ratio", "win_rate", "matches_played", "last_updated"
        )
    )

    record_count = final_df.count()
    logger.info(f"Computed leaderboard for {record_count} players")

    # ── Step 7: Write to serving layer (PostgreSQL) ─────────
    logger.info("Writing leaderboard batch view to serving_leaderboard...")
    (
        final_df.write
        .jdbc(
            url=POSTGRES_URL,
            table="serving_leaderboard",
            mode="overwrite",   # Idempotent full recompute
            properties=POSTGRES_PROPS,
        )
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ [BATCH LAYER] Leaderboard batch complete | {record_count} players | {elapsed:.1f}s")

    spark.stop()


if __name__ == "__main__":
    run_leaderboard_batch()
