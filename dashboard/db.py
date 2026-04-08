"""
dashboard/db.py
---------------
Read-only queries against PostgreSQL for the Streamlit dashboard.
All functions return pandas DataFrames.
"""

import os
import logging
from functools import lru_cache
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import psycopg2

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------
_DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER','postgres')}:"
    f"{os.getenv('POSTGRES_PASSWORD','stockpass123')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5433')}/"
    f"{os.getenv('POSTGRES_DB','stock_analysis')}"
)


@lru_cache(maxsize=1)
def _engine():
    return create_engine(_DB_URL, pool_pre_ping=True)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _query(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    try:
        with _engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as exc:
        logger.error("DB query failed: %s", exc)
        return pd.DataFrame()


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def get_tickers() -> list[str]:
    """Return sorted list of distinct tickers in processed_stock_data."""
    df = _query("SELECT DISTINCT ticker FROM processed_stock_data ORDER BY ticker")
    return df["ticker"].tolist() if not df.empty else []


def get_latest_prices() -> pd.DataFrame:
    """Return the most recent processed row per ticker."""
    sql = """
        SELECT DISTINCT ON (ticker)
            ticker, date_time, open, high, low, close, volume,
            daily_return, ma_7, ma_30, volatility, trend_label
        FROM processed_stock_data
        ORDER BY ticker, date_time DESC
    """
    return _query(sql)


def get_price_history(
    ticker: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Return full OHLCV + analytics history for a ticker within date range."""
    conditions = ["ticker = :ticker"]
    params: dict = {"ticker": ticker}

    if start_date:
        conditions.append("date_time >= :start_date")
        params["start_date"] = str(start_date)
    if end_date:
        conditions.append("date_time <= :end_date")
        params["end_date"] = str(end_date)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT ticker, date_time, open, high, low, close, volume,
               daily_return, ma_7, ma_30, volatility, trend_label
        FROM processed_stock_data
        WHERE {where}
        ORDER BY date_time ASC
    """
    df = _query(sql, params)
    if not df.empty:
        df["date_time"] = pd.to_datetime(df["date_time"], utc=True)
    return df


def get_recent_raw(ticker: Optional[str] = None, limit: int = 50) -> pd.DataFrame:
    """Return the most recent raw records, optionally filtered by ticker."""
    if ticker:
        sql = """
            SELECT ticker, date_time, open, high, low, close, volume, source, created_at
            FROM raw_stock_data
            WHERE ticker = :ticker
            ORDER BY date_time DESC
            LIMIT :limit
        """
        params = {"ticker": ticker, "limit": limit}
    else:
        sql = """
            SELECT ticker, date_time, open, high, low, close, volume, source, created_at
            FROM raw_stock_data
            ORDER BY date_time DESC
            LIMIT :limit
        """
        params = {"limit": limit}
    return _query(sql, params)


def get_ingestion_logs(limit: int = 100) -> pd.DataFrame:
    """Return recent ingestion log entries."""
    sql = """
        SELECT ticker, event_type, status, message, created_at
        FROM ingestion_logs
        ORDER BY created_at DESC
        LIMIT :limit
    """
    return _query(sql, {"limit": limit})


def get_trend_distribution(ticker: str) -> pd.DataFrame:
    """Return count of each trend_label for a ticker."""
    sql = """
        SELECT trend_label, COUNT(*) AS count
        FROM processed_stock_data
        WHERE ticker = :ticker
        GROUP BY trend_label
    """
    return _query(sql, {"ticker": ticker})


def get_data(symbol: str) -> pd.DataFrame:
    """
    Simple helper to fetch rows from `stock_prices` for the given symbol.

    Returns a DataFrame with columns: symbol, price, timestamp (may be empty).
    Uses psycopg2 and environment variables; default port is 5433.
    """
    host = os.getenv("POSTGRES_HOST", os.getenv("DB_HOST", "localhost"))
    port = int(os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5433")))
    db = os.getenv("POSTGRES_DB", os.getenv("DB_NAME", "stocks"))
    user = os.getenv("POSTGRES_USER", os.getenv("DB_USER", "postgres"))
    pwd = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", "postgres"))

    sql = "SELECT symbol, price, timestamp FROM stock_prices WHERE symbol = %s ORDER BY timestamp ASC"
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)
        df = pd.read_sql(sql, conn, params=(symbol,))
        conn.close()
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("UTC")
        return df
    except Exception as exc:
        logger.exception("Failed to read stock_prices for %s: %s", symbol, exc)
        return pd.DataFrame()