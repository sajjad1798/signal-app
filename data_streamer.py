import sqlite3
from sqlalchemy import create_engine
from websocket import WebSocketApp
import pandas as pd
import json

# Create SQLite database engine
engine = create_engine("sqlite:///CryptoDB.db")

# Binance WebSocket stream for all mini tickers
stream = "wss://stream.binance.com:9443/ws/!miniTicker@arr"


# WebSocket message handler
def on_message(ws, message):
    msg = json.loads(message)
    symbols = [x for x in msg if x["s"].endswith("USDT")]

    for symbol in symbols:
        symbol_name = symbol["s"]
        # Include all required fields
        frame = pd.DataFrame([symbol])[["E", "s", "c", "o", "h", "l", "v"]]
        frame["E"] = pd.to_datetime(
            frame["E"], unit="ms"
        )  # Convert event time to datetime
        frame["c"] = frame["c"].astype(float)  # Convert close price to float
        frame["o"] = frame["o"].astype(float)  # Convert open price to float
        frame["h"] = frame["h"].astype(float)  # Convert high price to float
        frame["l"] = frame["l"].astype(float)  # Convert low price to float
        frame["v"] = frame["v"].astype(float)  # Convert volume to float

        # Save to SQLite
        table_name = symbol_name
        frame[["E", "c", "o", "h", "l", "v"]].to_sql(
            table_name, engine, index=False, if_exists="append"
        )


# WebSocket handlers
def on_open(ws):
    print("WebSocket connection opened")


def on_close(ws, close_status_code, close_msg):
    print(
        f"WebSocket connection closed. Code: {close_status_code}, Message: {close_msg}"
    )


# Start WebSocket
def start_websocket():
    ws = WebSocketApp(stream, on_message=on_message, on_open=on_open, on_close=on_close)
    ws.run_forever()


if __name__ == "__main__":
    print("Starting WebSocket...")
    start_websocket()
