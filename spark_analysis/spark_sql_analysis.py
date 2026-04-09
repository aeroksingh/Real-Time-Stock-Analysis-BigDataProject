"""
spark_analysis/spark_sql_analysis.py
====================================== 
Stock Price Trend Analysis using Spark SQL
==========================================

Problem Statement #19 — Big Data Analytics with Hadoop & Spark

Workflow:
  1.  Fetch historical OHLCV data from Yahoo Finance
  2.  Load into Apache Spark (local mode — no cluster required for demo)
  3.  Register as SQL temp views
  4.  Run Spark SQL queries:
        Q1 – Summary statistics per ticker
        Q2 – Moving averages (MA-7, MA-30) via Window functions
        Q3 – Daily returns & rolling volatility
        Q4 – Trend classification (UP / DOWN / NEUTRAL)
        Q5 – Top 5 best & worst single-day returns
        Q6 – Volume spike detection
        Q7 – Save enriched dataset as Parquet (Hadoop-compatible)
  5.  Write CSVs + Parquet to output/ for dashboard consumption

Usage:
    python spark_analysis/spark_sql_analysis.py
    -- or --
    python -m spark_analysis.spark_sql_analysis
"""

import os
import sys
import logging
import random
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)

# ------------------------------------------------------------------ #
# Configuration                                                       #
# ------------------------------------------------------------------ #

TICKERS       = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
HISTORY_DAYS  = 180           # ~6 months of daily bars
SAMPLE_TICKER = "AAPL"        # used for per-ticker sample outputs

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("spark_sql_analysis")

# ------------------------------------------------------------------ #
# Step 1 – Fetch data from Yahoo Finance                              #
# ------------------------------------------------------------------ #

def _try_ticker_history(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """Strategy 1: yf.Ticker().history() — more resilient than yf.download()."""
    t = yf.Ticker(ticker)
    raw = t.history(start=start, end=end, interval="1d", auto_adjust=True)
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    return raw


def _try_download(ticker: str, start: str, end: str) -> pd.DataFrame | None:
    """Strategy 2: yf.download() with group_by workaround."""
    raw = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=True,
        threads=False,      # avoids thread-pool hangs on some Windows setups
    )
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    return raw


# Approximate seed prices for synthetic fallback (USD, ballpark 2025 levels)
_SEED_PRICES = {
    "AAPL": 175.0, "MSFT": 380.0, "GOOGL": 170.0,
    "AMZN": 185.0, "TSLA": 180.0,
}
_DEFAULT_SEED = 100.0


def _generate_synthetic(ticker: str, start_date, end_date) -> pd.DataFrame:
    """
    Strategy 3 (fallback): Geometric Brownian Motion synthetic OHLCV data.
    Used only when Yahoo Finance is completely unreachable so that the Spark
    demo can still run and showcase all SQL queries.
    """
    rng = np.random.default_rng(seed=abs(hash(ticker)) % (2**32))
    dates = pd.bdate_range(start=start_date, end=end_date)          # business days only
    n     = len(dates)
    mu, sigma = 0.0003, 0.015                                        # daily drift & vol
    seed_price = _SEED_PRICES.get(ticker, _DEFAULT_SEED)

    log_returns = rng.normal(mu, sigma, n)
    close       = seed_price * np.exp(np.cumsum(log_returns))
    high        = close * (1 + rng.uniform(0.002, 0.012, n))
    low         = close * (1 - rng.uniform(0.002, 0.012, n))
    open_       = low   + rng.uniform(0, 1, n) * (high - low)
    volume      = rng.integers(5_000_000, 50_000_000, n)

    return pd.DataFrame({
        "Open": open_, "High": high, "Low": low,
        "Close": close, "Volume": volume,
    }, index=dates)


def fetch_stock_data(tickers: list[str], days: int = HISTORY_DAYS) -> pd.DataFrame:
    """
    Download daily OHLCV bars for every ticker and return a single DataFrame.

    Tries three strategies in order:
      1. yf.Ticker().history()   — preferred; resilient to Yahoo API changes
      2. yf.download()           — classic approach as secondary attempt
      3. Synthetic GBM data      — demo fallback so Spark queries always run
    """
    end_date   = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    start_str, end_str = str(start_date), str(end_date)
    logger.info(
        "Fetching %d days of data (%s → %s) for: %s",
        days, start_date, end_date, tickers,
    )

    frames = []
    for ticker in tickers:
        raw = None

        # ── Strategy 1 ──────────────────────────────────────────────
        try:
            raw = _try_ticker_history(ticker, start_str, end_str)
            if raw is not None and not raw.empty:
                logger.info("[%s] Fetched via Ticker.history()  (%d rows).", ticker, len(raw))
        except Exception as exc:
            logger.debug("[%s] Ticker.history() failed: %s", ticker, exc)
            raw = None

        # ── Strategy 2 ──────────────────────────────────────────────
        if raw is None or raw.empty:
            try:
                raw = _try_download(ticker, start_str, end_str)
                if raw is not None and not raw.empty:
                    logger.info("[%s] Fetched via yf.download()  (%d rows).", ticker, len(raw))
            except Exception as exc:
                logger.debug("[%s] yf.download() failed: %s", ticker, exc)
                raw = None

        # ── Strategy 3 (synthetic fallback) ─────────────────────────
        if raw is None or raw.empty:
            logger.warning(
                "[%s] Yahoo Finance unreachable – using synthetic demo data.", ticker
            )
            raw = _generate_synthetic(ticker, start_date, end_date)

        # ── Normalise column names & shape ───────────────────────────
        raw = raw.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        raw["ticker"] = ticker
        raw["date"]   = pd.to_datetime(raw.index).strftime("%Y-%m-%d")
        raw = raw.reset_index(drop=True)[
            ["ticker", "date", "open", "high", "low", "close", "volume"]
        ]
        frames.append(raw)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["close"])
    logger.info("Total rows loaded: %d", len(combined))
    return combined


# ------------------------------------------------------------------ #
# Step 2 – Create Spark session                                       #
# ------------------------------------------------------------------ #

def create_spark_session() -> SparkSession:
    """
    Create a local SparkSession.
    local[*] uses all available CPU cores on this machine.
    No cluster or HDFS is required for this demo.

    IMPORTANT: PYSPARK_PYTHON must point to the same interpreter that is
    running this script.  Without this, PySpark 3.5.x spawns a worker
    using a *different* Python (or version) and the worker crashes
    immediately with "Python worker exited unexpectedly" / EOFException.
    This is the common failure mode on Python 3.12 with PySpark ≤ 3.5.0.
    """
    # Pin worker Python to the exact interpreter running this script.
    os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder
        .appName("StockPriceTrendAnalysis")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")   # low for demo
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        # Tell the executor side as well (belt-and-suspenders on Windows)
        .config("spark.pyspark.python",        sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ------------------------------------------------------------------ #
# Step 3 – Load pandas DataFrame into Spark and register SQL view    #
# ------------------------------------------------------------------ #

SCHEMA = StructType([
    StructField("ticker", StringType(), False),
    StructField("date",   StringType(), False),
    StructField("open",   DoubleType(), True),
    StructField("high",   DoubleType(), True),
    StructField("low",    DoubleType(), True),
    StructField("close",  DoubleType(), True),
    StructField("volume", LongType(),   True),
])


def load_into_spark(spark: SparkSession, pdf: pd.DataFrame):
    """Convert pandas → Spark DataFrame, cast date column, register as temp view."""
    sdf = spark.createDataFrame(pdf, schema=SCHEMA)
    sdf = sdf.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    sdf.createOrReplaceTempView("stock_raw")
    count = sdf.count()
    logger.info("Registered 'stock_raw' SQL view with %d rows.", count)
    return sdf


# ------------------------------------------------------------------ #
# Step 4 – Spark SQL queries                                          #
# ------------------------------------------------------------------ #

def run_all_queries(spark: SparkSession) -> pd.DataFrame:
    """
    Execute all Spark SQL queries and return the final enriched DataFrame.
    Results are also saved to output/.
    """
    sep = "=" * 62

    # ── Q1: Summary statistics ─────────────────────────────────────
    print(f"\n{sep}")
    print("Q1 — Summary Statistics per Ticker")
    print(sep)

    q1 = spark.sql("""
        SELECT
            ticker,
            COUNT(*)                        AS total_days,
            ROUND(MIN(close),  2)           AS min_close,
            ROUND(MAX(close),  2)           AS max_close,
            ROUND(AVG(close),  2)           AS avg_close,
            ROUND(AVG(volume) / 1e6, 2)     AS avg_volume_M
        FROM stock_raw
        GROUP BY ticker
        ORDER BY ticker
    """)
    q1.show()
    q1.toPandas().to_csv(OUTPUT_DIR / "q1_summary_stats.csv", index=False)

    # ── Q2: Moving averages (MA-7 & MA-30) ────────────────────────
    print(f"\n{sep}")
    print("Q2 — Moving Averages (MA-7 & MA-30) via Window Functions")
    print(sep)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW stock_with_ma AS
        SELECT
            ticker, date, open, high, low, close, volume,
            ROUND(
                AVG(close) OVER (
                    PARTITION BY ticker
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 4) AS ma_7,
            ROUND(
                AVG(close) OVER (
                    PARTITION BY ticker
                    ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 4) AS ma_30
        FROM stock_raw
    """)

    spark.sql(f"""
        SELECT ticker, date, ROUND(close,2) AS close, ma_7, ma_30
        FROM stock_with_ma
        WHERE ticker = '{SAMPLE_TICKER}'
        ORDER BY date DESC
        LIMIT 10
    """).show(truncate=False)

    # ── Q3: Daily returns & rolling volatility ─────────────────────
    print(f"\n{sep}")
    print("Q3 — Daily Returns & 7-Day Rolling Volatility")
    print(sep)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW stock_with_returns AS
        SELECT
            ticker, date, close, ma_7, ma_30,
            ROUND(
                (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
                / LAG(close) OVER (PARTITION BY ticker ORDER BY date) * 100
            , 4) AS daily_return_pct,
            ROUND(
                STDDEV(
                    (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
                    / LAG(close) OVER (PARTITION BY ticker ORDER BY date)
                ) OVER (
                    PARTITION BY ticker
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) * 100
            , 4) AS volatility_7d
        FROM stock_with_ma
    """)

    spark.sql(f"""
        SELECT ticker, date, ROUND(close,2) AS close,
               daily_return_pct AS return_pct, volatility_7d
        FROM stock_with_returns
        WHERE ticker = '{SAMPLE_TICKER}'
          AND daily_return_pct IS NOT NULL
        ORDER BY date DESC
        LIMIT 10
    """).show(truncate=False)

    # ── Q4: Trend classification ────────────────────────────────────
    print(f"\n{sep}")
    print("Q4 — Trend Classification  (UP / DOWN / NEUTRAL)")
    print(sep)
    print("Rule: close > MA-7 × 1.005 → UP")
    print("      close < MA-7 × 0.995 → DOWN   else NEUTRAL\n")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW stock_with_trend AS
        SELECT
            ticker, date, open, high, low, close, volume,
            ma_7, ma_30, daily_return_pct, volatility_7d,
            CASE
                WHEN close > ma_7 * 1.005 THEN 'UP'
                WHEN close < ma_7 * 0.995 THEN 'DOWN'
                ELSE 'NEUTRAL'
            END AS trend_label
        FROM stock_with_returns
    """)

    q4 = spark.sql("""
        SELECT ticker, trend_label,
               COUNT(*) AS days,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ticker), 1)
                   AS pct_of_ticker
        FROM stock_with_trend
        GROUP BY ticker, trend_label
        ORDER BY ticker, trend_label
    """)
    q4.show()
    q4.toPandas().to_csv(OUTPUT_DIR / "q4_trend_distribution.csv", index=False)

    # ── Q5: Top movers ─────────────────────────────────────────────
    print(f"\n{sep}")
    print("Q5 — Top 5 Best & Worst Single-Day Returns")
    print(sep)

    print("  BEST days:")
    spark.sql("""
        SELECT ticker, date, ROUND(close,2) AS close,
               daily_return_pct AS return_pct
        FROM stock_with_trend
        WHERE daily_return_pct IS NOT NULL
        ORDER BY daily_return_pct DESC
        LIMIT 5
    """).show(truncate=False)

    print("  WORST days:")
    spark.sql("""
        SELECT ticker, date, ROUND(close,2) AS close,
               daily_return_pct AS return_pct
        FROM stock_with_trend
        WHERE daily_return_pct IS NOT NULL
        ORDER BY daily_return_pct ASC
        LIMIT 5
    """).show(truncate=False)

    # ── Q6: Volume spike detection ─────────────────────────────────
    print(f"\n{sep}")
    print("Q6 — Volume Spike Detection  (volume > 2× ticker average)")
    print(sep)

    q6 = spark.sql("""
        SELECT ticker, date, volume,
               ROUND(avg_volume / 1e6, 2)  AS avg_volume_M,
               volume_ratio
        FROM (
            SELECT
                ticker, date, volume,
                ROUND(AVG(volume) OVER (PARTITION BY ticker), 0) AS avg_volume,
                ROUND(volume / AVG(volume) OVER (PARTITION BY ticker), 2)
                    AS volume_ratio
            FROM stock_raw
        ) t
        WHERE volume_ratio > 2.0
        ORDER BY volume_ratio DESC
        LIMIT 10
    """)
    q6.show(truncate=False)
    q6.toPandas().to_csv(OUTPUT_DIR / "q6_volume_spikes.csv", index=False)

    # ── Q7: Correlation between tickers ────────────────────────────
    print(f"\n{sep}")
    print("Q7 — Average Daily Return & Volatility Comparison")
    print(sep)

    q7 = spark.sql("""
        SELECT
            ticker,
            ROUND(AVG(daily_return_pct), 4)    AS avg_daily_return_pct,
            ROUND(MAX(volatility_7d),    4)    AS max_volatility_7d,
            ROUND(AVG(ABS(daily_return_pct)), 4) AS avg_abs_move
        FROM stock_with_trend
        WHERE daily_return_pct IS NOT NULL
        GROUP BY ticker
        ORDER BY avg_daily_return_pct DESC
    """)
    q7.show(truncate=False)
    q7.toPandas().to_csv(OUTPUT_DIR / "q7_return_vs_volatility.csv", index=False)

    # ── Q8: Save full enriched data as Parquet (Hadoop-compatible) ─
    print(f"\n{sep}")
    print("Q8 — Saving Enriched Dataset as Parquet  (Hadoop-compatible)")
    print(sep)

    final_df = spark.sql("""
        SELECT * FROM stock_with_trend
        ORDER BY ticker, date
    """)

    parquet_path = str(OUTPUT_DIR / "stock_analysis_parquet")
    final_df.write.mode("overwrite").partitionBy("ticker").parquet(parquet_path)
    logger.info("Parquet saved → %s/", parquet_path)

    csv_path = OUTPUT_DIR / "full_analysis.csv"
    final_df.toPandas().to_csv(csv_path, index=False)
    logger.info("CSV saved   → %s", csv_path)

    print(f"\nParquet (partitioned by ticker): {parquet_path}/")
    print(f"Full CSV:                         {csv_path}")

    return final_df.toPandas()


# ------------------------------------------------------------------ #
# Entry-point                                                         #
# ------------------------------------------------------------------ #

def main() -> None:
    banner = "=" * 62
    print(f"\n{banner}")
    print("  STOCK PRICE TREND ANALYSIS — Spark SQL Demo")
    print("  Problem Statement #19 | Big Data Analytics")
    print(f"{banner}\n")

    # 1. Fetch raw data from Yahoo Finance
    pdf = fetch_stock_data(TICKERS, days=HISTORY_DAYS)

    # 2. Start local Spark session
    spark = create_spark_session()
    logger.info("Spark version: %s", spark.version)

    # 3. Push data into Spark SQL
    load_into_spark(spark, pdf)

    # 4. Run all analytical queries
    final_pdf = run_all_queries(spark)

    # Done
    print(f"\n{'='*62}")
    print(f"  Demo complete!  {len(final_pdf)} enriched rows written to output/")
    print(f"{'='*62}\n")

    spark.stop()


if __name__ == "__main__":
    main()
