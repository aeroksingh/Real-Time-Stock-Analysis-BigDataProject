"""
producer/config.py
------------------
Centralised configuration loaded from environment variables / .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)


class ProducerConfig:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "stock_prices")

    # Stock tickers to track
    TICKERS: list[str] = [
        t.strip().upper()
        for t in os.getenv("TICKERS", "AAPL,MSFT,GOOGL,AMZN,TSLA").split(",")
        if t.strip()
    ]

    # Fetch interval in seconds
    FETCH_INTERVAL_SECONDS: int = int(os.getenv("FETCH_INTERVAL_SECONDS", "60"))

    # How many calendar days of history to pull on each cycle
    HISTORY_DAYS: int = int(os.getenv("HISTORY_DAYS", "90"))

    # yfinance data interval  ("1d" = daily bars)
    YFINANCE_INTERVAL: str = "1d"