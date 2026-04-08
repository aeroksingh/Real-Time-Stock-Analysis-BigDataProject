"""
consumer/processor.py
---------------------
Takes a raw message dict (or a DataFrame of a ticker's history) and:
  1. Validates required fields
  2. Computes analytics (daily_return, MA-7, MA-30, volatility, trend_label)

The processor works at the *batch* level – it receives all stored rows for
a given ticker plus the new incoming row, so moving averages are accurate.
"""

import logging
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Required keys in every raw message
REQUIRED_FIELDS = {"ticker", "date_time", "open", "high", "low", "close", "volume"}


def validate_message(msg: dict) -> bool:
    """Return True if the message contains all required fields and basic sanity."""
    missing = REQUIRED_FIELDS - set(msg.keys())
    if missing:
        logger.warning("Message missing fields %s: %s", missing, msg)
        return False

    for field in ("open", "high", "low", "close"):
        val = msg.get(field)
        if val is not None and (not isinstance(val, (int, float)) or val < 0):
            logger.warning("Invalid value for %s: %s", field, val)
            return False

    return True


def compute_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame sorted by date_time (oldest first) for a single ticker,
    append computed columns and return the enriched DataFrame.

    New columns:
        daily_return  – percentage change in close vs previous close
        ma_7          – 7-day simple moving average of close
        ma_30         – 30-day simple moving average of close
        volatility    – 7-day rolling std of daily_return (annualised proxy)
        trend_label   – "UP", "DOWN", or "NEUTRAL" based on close vs ma_7
    """
    df = df.sort_values("date_time").copy()

    # Daily return (%)
    df["daily_return"] = df["close"].pct_change()

    # Moving averages
    df["ma_7"]  = df["close"].rolling(window=7,  min_periods=1).mean()
    df["ma_30"] = df["close"].rolling(window=30, min_periods=1).mean()

    # 7-day rolling volatility of daily returns
    df["volatility"] = df["daily_return"].rolling(window=7, min_periods=1).std()

    # Trend label: compare latest close vs its MA-7
    def _label(row: pd.Series) -> str:
        if pd.isna(row["close"]) or pd.isna(row["ma_7"]):
            return "NEUTRAL"
        diff_pct = (row["close"] - row["ma_7"]) / row["ma_7"]
        if diff_pct > 0.005:      # > 0.5% above MA-7 → UP
            return "UP"
        elif diff_pct < -0.005:   # > 0.5% below MA-7 → DOWN
            return "DOWN"
        return "NEUTRAL"

    df["trend_label"] = df.apply(_label, axis=1)

    # Round analytics columns
    df["daily_return"] = df["daily_return"].round(6)
    df["ma_7"]         = df["ma_7"].round(4)
    df["ma_30"]        = df["ma_30"].round(4)
    df["volatility"]   = df["volatility"].round(6)

    # Replace NaN with None for safe SQL insertion
    df = df.where(pd.notnull(df), other=None)

    return df


def build_processed_record(raw_msg: dict, history_df: pd.DataFrame) -> Optional[dict]:
    """
    Given a single raw message and the historical DataFrame for that ticker
    (including the current row), return a processed record dict.

    Returns None if validation fails.
    """
    if not validate_message(raw_msg):
        return None

    # Append new row to history and recompute
    new_row = pd.DataFrame([raw_msg])
    new_row["date_time"] = pd.to_datetime(new_row["date_time"], utc=True)
    combined = pd.concat([history_df, new_row], ignore_index=True)
    combined = combined.drop_duplicates(subset=["ticker", "date_time"]).sort_values("date_time")

    enriched = compute_analytics(combined)

    # Return only the row that matches our new message's timestamp
    target_ts = pd.to_datetime(raw_msg["date_time"], utc=True)
    row = enriched[enriched["date_time"] == target_ts]
    if row.empty:
        logger.error("Could not find processed row for ts=%s", target_ts)
        return None

    row = row.iloc[0]

    processed = {
        "ticker":       row["ticker"],
        "date_time":    row["date_time"].isoformat(),
        "open":         row.get("open"),
        "high":         row.get("high"),
        "low":          row.get("low"),
        "close":        row.get("close"),
        "volume":       row.get("volume"),
        "daily_return": row.get("daily_return"),
        "ma_7":         row.get("ma_7"),
        "ma_30":        row.get("ma_30"),
        "volatility":   row.get("volatility"),
        "trend_label":  row.get("trend_label"),
    }
    return processed