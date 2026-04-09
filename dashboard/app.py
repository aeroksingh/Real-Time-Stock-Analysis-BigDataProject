"""
dashboard/app.py
-----------------
Streamlit dashboard for Stock Price Trend Analysis.

Supports two data sources (auto-detected):
  1. PostgreSQL  – live pipeline data (requires running docker-compose)
  2. CSV fallback – reads output/full_analysis.csv produced by spark_sql_analysis.py

Run:
    streamlit run dashboard/app.py
"""

import os
import logging
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

# ------------------------------------------------------------------ #
# Config                                                              #
# ------------------------------------------------------------------ #

_ENV = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV)

logger = logging.getLogger(__name__)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB   = os.getenv("POSTGRES_DB",   "stock_analysis")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "stockpass123")

CSV_FALLBACK  = Path(__file__).resolve().parent.parent / "output" / "full_analysis.csv"

TREND_COLORS  = {"UP": "#00c853", "DOWN": "#d50000", "NEUTRAL": "#ffab00"}
AUTO_REFRESH_SECS = 60


# ------------------------------------------------------------------ #
# Data loading                                                        #
# ------------------------------------------------------------------ #

def _load_from_postgres() -> pd.DataFrame:
    try:
        from sqlalchemy import create_engine, text
        url = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            df = pd.read_sql(
                text("""
                    SELECT ticker, date_time AS date, open, high, low, close, volume,
                           daily_return, ma_7, ma_30, volatility, trend_label
                    FROM processed_stock_data
                    ORDER BY date_time ASC
                """),
                conn,
            )
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
        df.rename(columns={"daily_return": "daily_return_pct", "volatility": "volatility_7d"}, inplace=True)
        return df
    except Exception as exc:
        logger.warning("PostgreSQL unavailable (%s). Falling back to CSV.", exc)
        return pd.DataFrame()


def _load_from_csv() -> pd.DataFrame:
    if not CSV_FALLBACK.exists():
        return pd.DataFrame()
    df = pd.read_csv(CSV_FALLBACK, parse_dates=["date"])
    return df


@st.cache_data(ttl=AUTO_REFRESH_SECS)
def load_data() -> tuple[pd.DataFrame, str]:
    df = _load_from_postgres()
    if not df.empty:
        return df, "PostgreSQL (live pipeline)"
    df = _load_from_csv()
    if not df.empty:
        return df, "CSV fallback (output/full_analysis.csv)"
    return pd.DataFrame(), "No data source available"


# ------------------------------------------------------------------ #
# Page layout                                                         #
# ------------------------------------------------------------------ #

st.set_page_config(page_title="Stock Trend Analysis", page_icon="📈", layout="wide")

st.title("📈 Stock Price Trend Analysis")
st.caption("Problem Statement #19 — Big Data Analytics with Hadoop & Spark")

df, source = load_data()

if df.empty:
    st.error(
        "No data found.\n\n"
        "**Option A (Spark demo only):** `python spark_analysis/spark_sql_analysis.py`\n\n"
        "**Option B (full pipeline):** `docker-compose up` → start producer & consumer"
    )
    st.stop()

st.info(f"Data source: **{source}** — {len(df):,} rows, {df['ticker'].nunique()} tickers")

# ------------------------------------------------------------------ #
# Sidebar                                                             #
# ------------------------------------------------------------------ #

tickers  = sorted(df["ticker"].unique().tolist())
selected = st.sidebar.selectbox("Select Ticker", tickers)

df["date"] = pd.to_datetime(df["date"])
min_date, max_date = df["date"].min().date(), df["date"].max().date()

date_range = st.sidebar.date_input("Date Range", value=(min_date, max_date),
                                   min_value=min_date, max_value=max_date)
show_ma7  = st.sidebar.checkbox("Show MA-7",  value=True)
show_ma30 = st.sidebar.checkbox("Show MA-30", value=True)

if st.sidebar.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

start_d, end_d = (date_range[0], date_range[1]) if len(date_range) == 2 else (min_date, max_date)
fdf = df[
    (df["ticker"] == selected) &
    (df["date"].dt.date >= start_d) &
    (df["date"].dt.date <= end_d)
].copy()

if fdf.empty:
    st.warning("No data for the selected ticker / date range.")
    st.stop()

# ------------------------------------------------------------------ #
# KPIs                                                                #
# ------------------------------------------------------------------ #

latest = fdf.iloc[-1]
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest Close", f"${latest['close']:.2f}",
          f"{latest.get('daily_return_pct', 0):.2f}%")
k2.metric("MA-7",         f"${latest['ma_7']:.2f}")
k3.metric("MA-30",        f"${latest['ma_30']:.2f}")
k4.metric("Volatility",   f"{latest.get('volatility_7d', 0):.4f}")
k5.metric("Trend",        latest.get("trend_label", "—"), delta_color="off")

st.divider()

# ------------------------------------------------------------------ #
# Price chart                                                         #
# ------------------------------------------------------------------ #

st.subheader(f"{selected} — Price & Moving Averages")
fig = go.Figure()
fig.add_trace(go.Scatter(x=fdf["date"], y=fdf["close"],
                         name="Close", line=dict(color="#1565c0", width=2)))
if show_ma7:
    fig.add_trace(go.Scatter(x=fdf["date"], y=fdf["ma_7"], name="MA-7",
                             line=dict(color="#fb8c00", width=1.5, dash="dot")))
if show_ma30:
    fig.add_trace(go.Scatter(x=fdf["date"], y=fdf["ma_30"], name="MA-30",
                             line=dict(color="#8e24aa", width=1.5, dash="dash")))
fig.update_layout(height=380, hovermode="x unified",
                  legend=dict(orientation="h", yanchor="bottom", y=1.02),
                  margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------ #
# Volume + Return                                                     #
# ------------------------------------------------------------------ #

c1, c2 = st.columns(2)
with c1:
    st.subheader("Volume")
    fig_v = px.bar(fdf, x="date", y="volume",
                   color_discrete_sequence=["#1565c0"], height=260)
    fig_v.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig_v, use_container_width=True)

with c2:
    st.subheader("Daily Return (%)")
    if "daily_return_pct" in fdf.columns:
        fdf["color"] = fdf["daily_return_pct"].apply(
            lambda x: "#00c853" if (x or 0) >= 0 else "#d50000")
        fig_r = px.bar(fdf, x="date", y="daily_return_pct",
                       color="color", color_discrete_map="identity", height=260)
        fig_r.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
        st.plotly_chart(fig_r, use_container_width=True)
    else:
        st.info("Daily return column not available.")

# ------------------------------------------------------------------ #
# Trend distribution                                                  #
# ------------------------------------------------------------------ #

st.subheader("Trend Distribution")
if "trend_label" in fdf.columns:
    cp, ct = st.columns([1, 2])
    with cp:
        tc = fdf["trend_label"].value_counts().reset_index()
        tc.columns = ["trend_label", "count"]
        fig_p = px.pie(tc, names="trend_label", values="count",
                       color="trend_label", color_discrete_map=TREND_COLORS, height=280)
        fig_p.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_p, use_container_width=True)
    with ct:
        st.markdown("**Recent Trend Labels (last 15 rows)**")
        recent = fdf[["date", "close", "ma_7", "trend_label"]].tail(15).sort_values("date", ascending=False)
        recent["close"] = recent["close"].round(2)
        recent["ma_7"]  = recent["ma_7"].round(4)
        st.dataframe(recent, use_container_width=True, hide_index=True)

# ------------------------------------------------------------------ #
# All tickers snapshot                                                #
# ------------------------------------------------------------------ #

st.divider()
st.subheader("All Tickers — Latest Snapshot")
snap_cols = ["ticker", "date", "close", "ma_7", "ma_30", "daily_return_pct", "trend_label"]
snap_cols = [c for c in snap_cols if c in df.columns]
latest_all = df.sort_values("date").groupby("ticker").last().reset_index()[snap_cols]
for col in ["close", "ma_7", "ma_30"]:
    if col in latest_all.columns:
        latest_all[col] = latest_all[col].round(2)
if "daily_return_pct" in latest_all.columns:
    latest_all["daily_return_pct"] = latest_all["daily_return_pct"].round(3)
st.dataframe(latest_all, use_container_width=True, hide_index=True)

st.caption(f"Cache TTL: {AUTO_REFRESH_SECS}s. Use Refresh button for immediate update.")
