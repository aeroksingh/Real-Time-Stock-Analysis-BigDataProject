import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px

# --------------------------
# DB Connection
# --------------------------
conn = psycopg2.connect(
    host="localhost",
    database="stock_db",
    user="postgres",
    password="singh",
    port=5433
)

# --------------------------
# Load Data
# --------------------------
def load_data():
    query = "SELECT * FROM stock_prices ORDER BY timestamp"
    return pd.read_sql(query, conn)

df = load_data()

# --------------------------
# UI
# --------------------------
st.title("📊 Real-Time Stock Dashboard")

stocks = df['symbol'].unique()
selected_stock = st.selectbox("Select Stock", stocks)

filtered_df = df[df['symbol'] == selected_stock]

# --------------------------
# Latest Price
# --------------------------
latest_price = filtered_df.iloc[-1]['price']
st.metric(label="Latest Price", value=f"${latest_price}")

# --------------------------
# Chart
# --------------------------
fig = px.line(
    filtered_df,
    x="timestamp",
    y="price",
    title=f"{selected_stock} Price Trend"
)

st.plotly_chart(fig)

# --------------------------
# Auto refresh
# --------------------------
st.experimental_rerun()