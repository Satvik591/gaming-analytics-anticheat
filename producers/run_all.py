"""
Run All Producers
─────────────────
Starts all 5 producers simultaneously using multiprocessing.
This simulates the full gaming event ecosystem.

Usage:
    cd producers
    python run_all.py
"""
import multiprocessing
import sys
from loguru import logger

from player_events_producer import run_producer as run_player_events
from match_logs_producer     import run_producer as run_match_logs
from chat_producer           import run_producer as run_chat
from purchases_producer      import run_producer as run_purchases
from game_state_producer     import run_producer as run_game_state


def start_all():
    producers = [
        ("Player Events",  run_player_events, {}),
        ("Match Logs",     run_match_logs,    {"interval_seconds": 30}),
        ("Chat Messages",  run_chat,          {}),
        ("Purchases",      run_purchases,     {"interval_seconds": 3}),
        ("Game State",     run_game_state,    {}),
    ]

    processes = []
    logger.info("🚀 Starting all 5 producers...")

    for name, func, kwargs in producers:
        p = multiprocessing.Process(target=func, kwargs=kwargs, name=name, daemon=True)
        p.start()
        processes.append(p)
        logger.info(f"  ✅ {name} producer started (PID: {p.pid})")

    logger.info("🎮 All producers running! Press Ctrl+C to stop.")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping all producers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
        logger.info("✅ All producers stopped cleanly.")


if __name__ == "__main__":
    start_all()
