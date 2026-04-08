"""
producer/main.py
----------------
Entry-point for the Kafka producer.

Workflow (repeated every FETCH_INTERVAL_SECONDS):
  1. For each configured ticker call stock_fetcher.fetch_ticker_data()
  2. Publish all records to the Kafka topic via kafka_producer.publish_records()
  3. Sleep until next cycle

Usage:
    python -m producer.main
    – or –
    python producer/main.py
"""

import logging
import sys
import time
from pathlib import Path

# Make sure the project root is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from producer.config import ProducerConfig
from producer.stock_fetcher import fetch_ticker_data
from producer.kafka_producer import create_producer, publish_records

# ------------------------------------------------------------------
# Logging setup
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("producer.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("producer.main")


def run() -> None:
    """Main producer loop."""
    logger.info("=== Stock Producer starting ===")
    logger.info("Tickers : %s", ProducerConfig.TICKERS)
    logger.info("Topic   : %s", ProducerConfig.KAFKA_TOPIC)
    logger.info("Interval: %ds", ProducerConfig.FETCH_INTERVAL_SECONDS)

    # Connect to Kafka (with retries)
    producer = create_producer(retries=10, delay=6)
    if producer is None:
        logger.critical("Unable to create Kafka producer. Exiting.")
        sys.exit(1)

    cycle = 0
    try:
        while True:
            cycle += 1
            logger.info("--- Cycle %d ---", cycle)

            for ticker in ProducerConfig.TICKERS:
                records = fetch_ticker_data(ticker, days=ProducerConfig.HISTORY_DAYS)
                if not records:
                    logger.warning("[%s] No records to publish.", ticker)
                    continue

                sent = publish_records(producer, ProducerConfig.KAFKA_TOPIC, records)
                logger.info("[%s] Published %d/%d records.", ticker, sent, len(records))

            logger.info("Cycle %d complete. Sleeping %ds…", cycle, ProducerConfig.FETCH_INTERVAL_SECONDS)
            time.sleep(ProducerConfig.FETCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        producer.close()
        logger.info("KafkaProducer closed.")


if __name__ == "__main__":
    run()