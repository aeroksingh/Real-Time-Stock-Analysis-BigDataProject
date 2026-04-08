"""
consumer/config.py
------------------
Configuration for the Kafka consumer and PostgreSQL writer.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


class ConsumerConfig:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "stock_prices")
    KAFKA_GROUP_ID: str          = os.getenv("KAFKA_GROUP_ID", "stock_consumer_group")
    CONSUMER_POLL_TIMEOUT_MS: int = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "5000"))

    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5433")
    POSTGRES_DB:   str = os.getenv("POSTGRES_DB",   "stock_analysis")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASSWORD", "stockpass123")

    @classmethod
    def db_url(cls) -> str:
        return (
            f"postgresql+psycopg2://{cls.POSTGRES_USER}:{cls.POSTGRES_PASS}"
            f"@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
        )