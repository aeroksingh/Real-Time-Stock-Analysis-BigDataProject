import sys
from pathlib import Path

import streamlit as st
import pandas as pd

# Ensure dashboard package imports work when running from project root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from db import get_data
from charts import plot_price_trend

st.set_page_config(page_title="Stock Dashboard", layout="wide")
st.title("📈 Real-time Stock Price Dashboard")

symbol = st.selectbox("Select Stock", ["AAPL", "GOOG", "MSFT"])

# Sidebar controls
interval = st.sidebar.slider("Auto-refresh interval (seconds)", min_value=2, max_value=60, value=5)
auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)

if auto_refresh:
	# inject a one-shot reload that runs after `interval` seconds
	import streamlit.components.v1 as components

	components.html(
		f"<script>setTimeout(()=>{{location.reload();}}, {int(interval * 1000)});</script>",
		height=0,
	)

try:
	data = get_data(symbol)
except Exception as exc:
	st.error(f"Error reading data: {exc}")
	st.stop()

if data is None or data.empty:
	st.warning("No data available yet. Please wait for the producer to send data.")
else:
	# Normalize and sort
	data = data.sort_values("timestamp")
	if "timestamp" in data.columns:
		data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_convert("UTC")

	latest = data.iloc[-1]
	latest_price = float(latest["price"])
	prev_price = float(data["price"].iloc[-2]) if len(data) >= 2 else None
	delta = (latest_price - prev_price) if prev_price is not None else None

	col_main, col_side = st.columns([3, 1])
	with col_main:
		st.subheader(f"{symbol} price trend")
		fig = plot_price_trend(data)
		st.plotly_chart(fig, use_container_width=True)

	with col_side:
		st.metric("Latest price", f"{latest_price:.2f}", delta=(f"{delta:+.2f}" if delta is not None else None))
		st.markdown("---")
		st.write("Last update:")
		st.write(pd.to_datetime(latest["timestamp"]).strftime("%Y-%m-%d %H:%M:%S %Z"))
