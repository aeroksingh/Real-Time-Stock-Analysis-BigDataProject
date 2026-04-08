"""
consumer/main.py
----------------
Entry-point for the Kafka consumer.

Workflow per message:
  1. Deserialise JSON message from Kafka
  2. Validate schema
  3. Upsert raw record into PostgreSQL
  4. Load ticker history from PostgreSQL
  5. Compute analytics (processor.py)
  6. Upsert processed record into PostgreSQL
  7. Log the event

Usage:
    python -m consumer.main
    – or –
    python consumer/main.py
"""

import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from consumer.config import ConsumerConfig
from consumer.processor import validate_message, build_processed_record
from consumer.db_writer import (
    upsert_raw,
    upsert_processed,
    load_ticker_history,
    log_event,
)

# Also provide a lightweight table for dashboards
from consumer.db_writer import insert_stock_price

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("consumer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("consumer.main")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def create_consumer(retries: int = 10, delay: int = 6) -> KafkaConsumer:
    """Create KafkaConsumer with retries."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                ConsumerConfig.KAFKA_TOPIC,
                bootstrap_servers=ConsumerConfig.KAFKA_BOOTSTRAP_SERVERS,
                group_id=ConsumerConfig.KAFKA_GROUP_ID,
                auto_offset_reset="latest", 
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=ConsumerConfig.CONSUMER_POLL_TIMEOUT_MS,
            )
            logger.info(
                "KafkaConsumer connected. Topic=%s Group=%s",
                ConsumerConfig.KAFKA_TOPIC,
                ConsumerConfig.KAFKA_GROUP_ID,
            )
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka not available (attempt %d/%d). Retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
        except KafkaError as exc:
            logger.error("KafkaError attempt %d: %s", attempt, exc)
            time.sleep(delay)

    logger.critical("Could not connect to Kafka after %d attempts. Exiting.", retries)
    sys.exit(1)


def handle_message(msg: dict) -> None:
    """Process a single Kafka message end-to-end."""
    ticker = msg.get("ticker", "UNKNOWN")

    # --- 1. Validate ---
    if not validate_message(msg):
        log_event(ticker, "VALIDATION", "FAILED", f"Schema validation failed: {msg}")
        return

    # --- 2. Upsert raw ---
    raw_ok = upsert_raw(msg)
    if not raw_ok:
        log_event(ticker, "RAW_INSERT", "ERROR", f"Failed to insert raw for {msg.get('date_time')}")
        return

    # --- 3. Load history for rolling metrics ---
    history = load_ticker_history(ticker)

    # --- 4. Compute analytics ---
    processed = build_processed_record(msg, history)
    if processed is None:
        log_event(ticker, "PROCESSING", "ERROR", f"Analytics computation failed for {msg.get('date_time')}")
        return

    # --- 5. Upsert processed ---
    proc_ok = upsert_processed(processed)
    status  = "SUCCESS" if proc_ok else "ERROR"
    log_event(
        ticker,
        "PROCESSED_INSERT",
        status,
        f"date_time={processed.get('date_time')} trend={processed.get('trend_label')}",
    )

    if proc_ok:
        logger.info(
            "[%s] %s  close=%.4f  trend=%s  ma7=%.4f",
            ticker,
            processed["date_time"],
            processed["close"] or 0,
            processed["trend_label"],
            processed["ma_7"] or 0,
        )
        # Write a simple price point for dashboard consumption
        try:
            ins_ok = insert_stock_price(processed["ticker"], processed["close"], processed["date_time"])
            if not ins_ok:
                logger.warning("Failed to write simple stock_prices row for %s", processed.get("ticker"))
        except Exception:
            logger.exception("Unexpected error inserting stock_prices for %s", processed.get("ticker"))


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------

def run() -> None:
    logger.info("=== Stock Consumer starting ===")

    consumer = create_consumer()

    try:
        while True:
            # KafkaConsumer is iterable; consumer_timeout_ms makes it
            # raise StopIteration when idle, which we catch below.
            try:
                for message in consumer:
                    handle_message(message.value)
            except StopIteration:
                # No messages within poll window – keep looping
                logger.debug("No new messages. Polling again…")
                continue

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        logger.info("KafkaConsumer closed.")


if __name__ == "__main__":
    run()