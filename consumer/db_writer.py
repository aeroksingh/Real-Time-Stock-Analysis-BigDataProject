"""
consumer/db_writer.py
----------------------
Handles all PostgreSQL interactions:
  - Upsert raw records into raw_stock_data
  - Upsert processed records into processed_stock_data
  - Insert rows into ingestion_logs
  - Load historical close prices for analytics computation
"""
import math

def _clean_record(record: dict) -> dict:
    """
    Convert:
    - numpy types → native Python types
    - NaN → None (for PostgreSQL)
    """
    cleaned = {}

    for k, v in record.items():
        # Convert numpy → Python
        if hasattr(v, "item"):
            v = v.item()

        # Convert NaN → None
        if isinstance(v, float) and math.isnan(v):
            v = None

        cleaned[k] = v

    return cleaned

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from consumer.config import ConsumerConfig

logger = logging.getLogger(__name__)


def _get_engine():
    """Return a cached SQLAlchemy engine."""
    return create_engine(
        ConsumerConfig.db_url(),
        pool_pre_ping=True,    # check connection before use
        pool_size=5,
        max_overflow=10,
    )


# Module-level engine (created once)
_engine = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = _get_engine()
    return _engine


# ------------------------------------------------------------------
# RAW DATA
# ------------------------------------------------------------------

def upsert_raw(record: dict) -> bool:
    """
    Insert a raw record.  Silently skips duplicates (ticker + date_time).
    Returns True on success, False on error.
    """
    sql = text("""
        INSERT INTO raw_stock_data
            (ticker, date_time, open, high, low, close, volume, source)
        VALUES
            (:ticker, :date_time, :open, :high, :low, :close, :volume, :source)
        ON CONFLICT (ticker, date_time) DO NOTHING
    """)
    try:
        with get_engine().connect() as conn:
            clean = _clean_record(record)   # ✅ ADD THIS
            conn.execute(sql, clean)        # ✅ USE CLEAN DATA
            conn.commit()
        return True
    except SQLAlchemyError as exc:
        logger.error("upsert_raw failed for %s/%s: %s", record.get("ticker"), record.get("date_time"), exc)
        return False


# ------------------------------------------------------------------
# PROCESSED DATA
# ------------------------------------------------------------------

def upsert_processed(record: dict) -> bool:
    """
    Upsert a processed record into processed_stock_data.
    On conflict (ticker + date_time) updates all analytics columns.
    Returns True on success, False on error.
    """
    sql = text("""
        INSERT INTO processed_stock_data
            (ticker, date_time, open, high, low, close, volume,
             daily_return, ma_7, ma_30, volatility, trend_label)
        VALUES
            (:ticker, :date_time, :open, :high, :low, :close, :volume,
             :daily_return, :ma_7, :ma_30, :volatility, :trend_label)
        ON CONFLICT (ticker, date_time) DO UPDATE SET
            open         = EXCLUDED.open,
            high         = EXCLUDED.high,
            low          = EXCLUDED.low,
            close        = EXCLUDED.close,
            volume       = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            ma_7         = EXCLUDED.ma_7,
            ma_30        = EXCLUDED.ma_30,
            volatility   = EXCLUDED.volatility,
            trend_label  = EXCLUDED.trend_label
    """)
    try:
        with get_engine().connect() as conn:
            clean = _clean_record(record)   # ✅ ADD THIS
            conn.execute(sql, clean)
            conn.commit()
        return True
    except SQLAlchemyError as exc:
        logger.error(
            "upsert_processed failed for %s/%s: %s",
            record.get("ticker"), record.get("date_time"), exc,
        )
        return False


# ------------------------------------------------------------------
# INGESTION LOGS
# ------------------------------------------------------------------

def log_event(
    ticker: Optional[str],
    event_type: str,
    status: str,
    message: str,
) -> None:
    """Append a row to ingestion_logs (fire-and-forget)."""
    sql = text("""
        INSERT INTO ingestion_logs (ticker, event_type, status, message)
        VALUES (:ticker, :event_type, :status, :message)
    """)
    try:
        with get_engine().connect() as conn:
            clean = _clean_record({
                "ticker": ticker,
                "event_type": event_type,
                "status": status,
                "message": message,
                
            })
            conn.execute(sql, clean)
            conn.commit()
    except SQLAlchemyError as exc:
        # Don't raise – logging should never break the pipeline
        logger.warning("Could not write to ingestion_logs: %s", exc)


# ------------------------------------------------------------------
# HISTORY LOADER (for analytics computation)
# ------------------------------------------------------------------

def load_ticker_history(ticker: str) -> pd.DataFrame:
    """
    Return all rows from raw_stock_data for *ticker* as a DataFrame,
    sorted by date_time ascending.  Used to compute rolling metrics.
    """
    sql = text("""
        SELECT ticker, date_time, open, high, low, close, volume
        FROM   raw_stock_data
        WHERE  ticker = :ticker
        ORDER  BY date_time ASC
    """)
    try:
        with get_engine().connect() as conn:
            df = pd.read_sql(sql, conn, params={"ticker": ticker})
        if not df.empty:
            df["date_time"] = pd.to_datetime(df["date_time"], utc=True)
        return df
    except SQLAlchemyError as exc:
        logger.error("load_ticker_history failed for %s: %s", ticker, exc)
        return pd.DataFrame()


# ------------------------------------------------------------------
# Simple stock_prices writer (for dashboard)
# ------------------------------------------------------------------
def insert_stock_price(symbol: str, price: float, timestamp: str) -> bool:
    """
    Insert or update a single price point into `stock_prices`.

    Parameters
    - symbol: ticker symbol (e.g. 'AAPL')
    - price:  closing price as float
    - timestamp: ISO8601 string for the sample timestamp

    Returns True on success, False on error.
    """
    sql = text("""
        INSERT INTO stock_prices (symbol, price, timestamp)
        VALUES (:symbol, :price, :timestamp)
        ON CONFLICT (symbol, timestamp) DO UPDATE SET
            price = EXCLUDED.price
    """)
    try:
        with get_engine().connect() as conn:
            conn.execute(sql, {
                "symbol": symbol,
                "price": price,
                "timestamp": timestamp,
            })
            conn.commit()
        return True
    except SQLAlchemyError as exc:
        logger.error("insert_stock_price failed for %s/%s: %s", symbol, timestamp, exc)
        return False