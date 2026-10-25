"""
ML Anti-Cheat Model Training (Batch Layer)
══════════════════════════════════════════
Runs daily as a Spark batch job.
Trains an Isolation Forest anomaly detection model on historical
game_state data to detect sophisticated cheaters that rule-based
detection misses.

Model is saved to disk and picked up by the Flink speed layer
for real-time scoring.

Run:
    spark-submit --master local[*] ml_training.py
"""
import os
import joblib
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from loguru import logger

POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:5432/{os.getenv('POSTGRES_DB','gaming_analytics')}"
POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER", "gameadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "gamepass123"),
    "driver": "org.postgresql.Driver",
}
MODEL_OUTPUT_PATH = os.getenv("MODEL_PATH", "ml/model/anticheat_model.pkl")

FEATURE_COLS = [
    "velocity", "headshot_rate", "reaction_time_ms",
    "accuracy_pct", "kills", "health"
]


def run_ml_training():
    logger.info("🤖 [ML TRAINING] Starting Isolation Forest training...")
    start_time = datetime.now()

    spark = SparkSession.builder \
        .appName("GamingAnalytics_MLTraining") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── Load game state data ─────────────────────────────────
    logger.info("Loading raw_game_state from PostgreSQL...")
    df = spark.read.jdbc(url=POSTGRES_URL, table="raw_game_state", properties=POSTGRES_PROPS)

    # ── Feature engineering ──────────────────────────────────
    features_df = df.select(FEATURE_COLS).dropna()
    row_count = features_df.count()
    logger.info(f"Training on {row_count} game state records")

    if row_count < 100:
        logger.warning("Not enough data to train model. Need at least 100 records.")
        spark.stop()
        return

    # Collect to driver for sklearn training
    X = np.array(features_df.collect())

    # ── Build ML pipeline ────────────────────────────────────
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model", IsolationForest(
            n_estimators=200,
            contamination=0.05,   # Expected 5% cheaters
            random_state=42,
            n_jobs=-1,
        ))
    ])

    logger.info("Training Isolation Forest model...")
    pipeline.fit(X)

    # ── Save model ───────────────────────────────────────────
    os.makedirs(os.path.dirname(MODEL_OUTPUT_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_OUTPUT_PATH)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ Model trained and saved to {MODEL_OUTPUT_PATH} | {elapsed:.1f}s")

    spark.stop()


if __name__ == "__main__":
    run_ml_training()
