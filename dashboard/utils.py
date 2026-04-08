"""
dashboard/utils.py
------------------
Utility helpers for the Streamlit dashboard.
"""

import io
import pandas as pd


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Convert a DataFrame to UTF-8 CSV bytes for st.download_button."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def format_large_number(n) -> str:
    """Format large integers with K/M/B suffix."""
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "–"
    n = int(n)
    if abs(n) >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if abs(n) >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if abs(n) >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def trend_badge(label: str) -> str:
    """Return an emoji badge for a trend label."""
    return {"UP": "🟢 UP", "DOWN": "🔴 DOWN", "NEUTRAL": "🟡 NEUTRAL"}.get(label, "–")


def pct_str(val) -> str:
    """Format a fraction as a percentage string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "–"
    return f"{float(val)*100:+.2f}%"