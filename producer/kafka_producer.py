"""
producer/kafka_producer.py
--------------------------
Thin wrapper around kafka-python KafkaProducer.
Handles serialisation, retries, and clean shutdown.
"""

import json
import logging
import time
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from producer.config import ProducerConfig

logger = logging.getLogger(__name__)


def _json_serialiser(value: dict) -> bytes:
    """Serialise a dict to UTF-8 JSON bytes."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def create_producer(retries: int = 5, delay: int = 5) -> Optional[KafkaProducer]:
    """
    Create and return a KafkaProducer.
    Retries up to *retries* times with *delay* seconds between attempts.
    Returns None if all attempts fail.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=ProducerConfig.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=_json_serialiser,
                acks="all",             # wait for full ISR acknowledgment
                retries=3,
                linger_ms=10,           # small batching window
                compression_type="gzip",
            )
            logger.info("KafkaProducer connected to %s", ProducerConfig.KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not available (attempt %d/%d). Retrying in %ds…",
                attempt, retries, delay,
            )
            time.sleep(delay)
        except KafkaError as exc:
            logger.error("KafkaError on attempt %d: %s", attempt, exc)
            time.sleep(delay)

    logger.error("Could not connect to Kafka after %d attempts.", retries)
    return None


def publish_records(
    producer: KafkaProducer,
    topic: str,
    records: list[dict],
) -> int:
    """
    Publish a list of record dicts to *topic*.

    Returns the number of successfully sent messages.
    """
    sent = 0
    for record in records:
        try:
            future = producer.send(topic, value=record)
            future.get(timeout=10)   # block until broker ack
            sent += 1
        except KafkaError as exc:
            logger.error("Failed to publish record %s: %s", record.get("date_time"), exc)

    producer.flush()
    return sent