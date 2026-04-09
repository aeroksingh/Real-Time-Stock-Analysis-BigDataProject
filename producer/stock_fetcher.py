"""
producer/stock_fetcher.py
--------------------------
Fetches OHLCV data from Yahoo Finance using yfinance and returns
a list of normalised dictionaries ready for Kafka serialisation.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import yfinance as yf
import pandas as pd

from producer.config import ProducerConfig

logger = logging.getLogger(__name__)


def fetch_ticker_data(
    ticker: str,
    days: int = ProducerConfig.HISTORY_DAYS,
) -> Optional[list[dict]]:
    """c
    Download daily OHLCV bars for *ticker* covering the last *days* calendar days.

    Returns
    -------
    list[dict] or None
        Each dict has keys: ticker, date_time, open, high, low, close,
        volume, source.  Returns None on failure.
    """
    end_date   = datetime.now(tz=timezone.utc).date()
    start_date = end_date - timedelta(days=days)

    try:
        raw: pd.DataFrame = yf.download(
            ticker,
            start=str(start_date),
            end=str(end_date),
            interval=ProducerConfig.YFINANCE_INTERVAL,
            progress=False,
            auto_adjust=True,   # adjusted OHLCV
        )
    except Exception as exc:
        logger.error("[%s] yfinance download failed: %s", ticker, exc)
        return None

    if raw is None or raw.empty:
        logger.warning("[%s] No data returned from yfinance.", ticker)
        return None

    # Flatten MultiIndex columns if present (yfinance >=0.2.x)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    # Rename columns to lower-case standard names
    raw = raw.rename(columns={
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume",
    })

    required = {"open", "high", "low", "close", "volume"}
    missing  = required - set(raw.columns)
    if missing:
        logger.error("[%s] Missing columns after rename: %s", ticker, missing)
        return None

    records = []
    for ts, row in raw.iterrows():
        # ts is a pandas Timestamp; convert to ISO-8601 UTC string
        dt_utc = pd.Timestamp(ts).tz_localize("UTC") if ts.tzinfo is None else pd.Timestamp(ts).tz_convert("UTC")
        record = {
            "ticker":    ticker,
            "date_time": dt_utc.isoformat(),
            "open":      round(float(row["open"]),   4) if not pd.isna(row["open"])   else None,
            "high":      round(float(row["high"]),   4) if not pd.isna(row["high"])   else None,
            "low":       round(float(row["low"]),    4) if not pd.isna(row["low"])    else None,
            "close":     round(float(row["close"]),  4) if not pd.isna(row["close"])  else None,
            "volume":    int(row["volume"])              if not pd.isna(row["volume"]) else None,
            "source":    "yfinance",
        }
        records.append(record)

    logger.info("[%s] Fetched %d records (%s → %s).", ticker, len(records), start_date, end_date)
    print("Fetching stock from Yahoo Finance...")
    return records