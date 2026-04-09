import yfinance as yf
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

stocks = ["AAPL", "MSFT", "TSLA"]

while True:
    for stock in stocks:
        data = yf.Ticker(stock).history(period="1d", interval="1m")
        latest = data.tail(1)

        if not latest.empty:
            price = float(latest["Close"].values[0])

            message = {
                "symbol": stock,
                "price": price
            }

            producer.send("stock-data", message)
            print(f"Sent: {message}")

    time.sleep(5)