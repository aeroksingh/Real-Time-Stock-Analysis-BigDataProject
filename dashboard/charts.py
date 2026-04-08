"""
dashboard/charts.py
-------------------
All Plotly figure factories used by the Streamlit dashboard.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Shared colour palette
_CLOSE_COLOR   = "#00C9A7"
_MA7_COLOR     = "#FFC75F"
_MA30_COLOR    = "#F96167"
_VOL_COLOR     = "#845EC2"
_NEUTRAL_COLOR = "#94A3B8"
_TREND_COLORS  = {"UP": "#22C55E", "DOWN": "#EF4444", "NEUTRAL": _NEUTRAL_COLOR}


def _base_layout(title: str) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=16, color="#E2E8F0")),
        paper_bgcolor="#0F172A",
        plot_bgcolor="#1E293B",
        font=dict(color="#94A3B8"),
        xaxis=dict(gridcolor="#334155", showgrid=True),
        yaxis=dict(gridcolor="#334155", showgrid=True),
        legend=dict(bgcolor="#1E293B", bordercolor="#334155", borderwidth=1),
        margin=dict(l=40, r=20, t=50, b=40),
        hovermode="x unified",
    )


def closing_price_chart(df: pd.DataFrame, ticker: str) -> go.Figure:
    """Candlestick + closing line chart."""
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df["date_time"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name="OHLC",
        increasing_line_color="#22C55E",
        decreasing_line_color="#EF4444",
    ))

    layout = _base_layout(f"{ticker} – Candlestick Chart")
    layout["xaxis_rangeslider_visible"] = False
    fig.update_layout(**layout)
    return fig


def moving_average_chart(df: pd.DataFrame, ticker: str) -> go.Figure:
    """Close price with MA-7 and MA-30 overlaid."""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["date_time"], y=df["close"],
        name="Close", line=dict(color=_CLOSE_COLOR, width=1.5),
        mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=df["date_time"], y=df["ma_7"],
        name="MA-7", line=dict(color=_MA7_COLOR, width=1.5, dash="dot"),
        mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=df["date_time"], y=df["ma_30"],
        name="MA-30", line=dict(color=_MA30_COLOR, width=1.5, dash="dash"),
        mode="lines",
    ))

    fig.update_layout(**_base_layout(f"{ticker} – Moving Averages"))
    return fig


def volatility_chart(df: pd.DataFrame, ticker: str) -> go.Figure:
    """7-day rolling volatility chart."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date_time"], y=df["volatility"],
        name="Volatility (7d)", line=dict(color=_VOL_COLOR, width=1.5),
        fill="tozeroy", fillcolor="rgba(132,94,194,0.15)",
        mode="lines",
    ))
    fig.update_layout(**_base_layout(f"{ticker} – 7-Day Rolling Volatility"))
    return fig


def daily_return_chart(df: pd.DataFrame, ticker: str) -> go.Figure:
    """Bar chart of daily returns coloured by positive/negative."""
    colors = df["daily_return"].apply(
        lambda r: "#22C55E" if (r is not None and r > 0) else "#EF4444"
    )
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date_time"], y=df["daily_return"],
        name="Daily Return",
        marker_color=colors,
    ))
    fig.update_layout(**_base_layout(f"{ticker} – Daily Return (%)"))
    return fig


def trend_pie_chart(trend_df: pd.DataFrame, ticker: str) -> go.Figure:
    """Donut chart showing distribution of trend labels."""
    if trend_df.empty:
        fig = go.Figure()
        fig.update_layout(**_base_layout(f"{ticker} – Trend Distribution"))
        return fig

    labels = trend_df["trend_label"].tolist()
    values = trend_df["count"].tolist()
    colors = [_TREND_COLORS.get(l, _NEUTRAL_COLOR) for l in labels]

    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.55,
        marker=dict(colors=colors),
        textfont=dict(color="#E2E8F0"),
    ))
    layout = _base_layout(f"{ticker} – Trend Distribution")
    layout["showlegend"] = True
    fig.update_layout(**layout)
    return fig


def plot_price_trend(df: pd.DataFrame, title: str | None = None) -> go.Figure:
    """Simple time-series price chart used by the lightweight dashboard.

    Expects DataFrame with columns `timestamp` and `price`.
    """
    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(**_base_layout(title or "Price Trend"))
        return fig

    x = df["timestamp"] if "timestamp" in df.columns else df.index
    y = df["price"] if "price" in df.columns else df["close"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x,
        y=y,
        mode="lines",
        name="Price",
        line=dict(color=_CLOSE_COLOR, width=2),
    ))

    fig.update_layout(**_base_layout(title or "Price Trend"))
    fig.update_xaxes(showgrid=True)
    fig.update_yaxes(showgrid=True)
    return fig