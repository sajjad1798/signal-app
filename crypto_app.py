import sqlite3
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
from ta.trend import EMAIndicator
import requests
import time
import schedule

# Create SQLite database engine
engine = create_engine("sqlite:///CryptoDB.db")

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "7717260550:AAEfClasfGBAS6zmQj0waLUTQqQeyECyr1s"
TELEGRAM_CHAT_ID = "1638869534"


# Function to send message to Telegram
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Signal sent to Telegram successfully.")
        else:
            print(f"Failed to send message to Telegram: {response.text}")
    except Exception as e:
        print(f"Error sending message to Telegram: {e}")


# Aggregate second-by-second data into 5-minute candles
def aggregate_to_5min(df):
    df = df.sort_values("E")
    df = df.set_index("E")
    df = (
        df.resample("5T")  # '5T' means 5-minute intervals
        .agg(
            {
                "o": "first",
                "h": "max",
                "l": "min",
                "c": "last",
                "v": "sum",
            }
        )
        .dropna()
    )
    df = df.reset_index()
    return df


# Analyze a single coin for EMA crossovers
def analyze_coin_for_crossover(symbol, engine):
    query = f"""
    SELECT * FROM "{symbol}"
    ORDER BY E ASC
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print(f"{symbol}: No data available.")
        return None

    df["E"] = pd.to_datetime(df["E"])
    df = aggregate_to_5min(df)  # Aggregate to 5-minute intervals

    if len(df) < 200:  # Ensure enough data for EMA 200 calculation
        print(f"{symbol}: Not enough data after aggregation for EMA calculations.")
        return None

    close_prices = df["c"]

    # Calculate EMAs
    df["EMA_50"] = EMAIndicator(close=close_prices, window=50).ema_indicator()
    df["EMA_200"] = EMAIndicator(close=close_prices, window=200).ema_indicator()

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # Check for bullish crossover with price condition
    if (
        prev["EMA_50"] <= prev["EMA_200"]
        and latest["EMA_50"] > latest["EMA_200"]
        and latest["c"] > latest["EMA_50"]  # Price is above EMA 50
    ):
        return {
            "symbol": symbol,
            "status": "bullish crossover",
            "entry_condition": "long position",
        }

    # Check for bearish crossover with price condition
    if (
        prev["EMA_50"] >= prev["EMA_200"]
        and latest["EMA_50"] < latest["EMA_200"]
        and latest["c"] < latest["EMA_50"]  # Price is below EMA 50
    ):
        return {
            "symbol": symbol,
            "status": "bearish crossover",
            "entry_condition": "short position",
        }

    return None  # No valid signal


# Analyze all coins in the database
def analyze_all_coins_for_crossover():
    tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
    tables = pd.read_sql(tables_query, engine)["name"].tolist()

    results = []
    for table in tables:
        result = analyze_coin_for_crossover(table, engine)
        if result:
            results.append(result)

    return results


# Function to run the crossover analysis
def run_crossover_analysis():
    print("Running EMA crossover analysis...")
    results = analyze_all_coins_for_crossover()

    if results:
        print("Detected the following crossover events:")
        message = "EMA Crossover Signals Detected:\n\n"
        for result in results:
            signal = f"- {result['symbol']}: {result['status']} | Entry Condition: {result['entry_condition']}"
            message += signal + "\n"
            print(signal)
        send_telegram_message(message)
    else:
        print("No crossover events detected.")


# Cleanup function to delete old data
def cleanup_old_data(engine):
    print("Running database cleanup...")
    connection = engine.connect()
    twelve_hours_ago = datetime.now() - timedelta(hours=12)
    timestamp_cutoff = int(
        twelve_hours_ago.timestamp() * 1000
    )  # Convert to milliseconds

    tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
    tables = pd.read_sql(tables_query, engine)["name"].tolist()

    for table in tables:
        delete_query = f"""
        DELETE FROM "{table}" WHERE E < {timestamp_cutoff}
        """
        connection.execute(delete_query)
        print(f"Cleaned up old data from table: {table}")

    connection.close()


# Schedule the analysis every 5 minutes
schedule.every(5).minutes.do(run_crossover_analysis)

# Schedule cleanup every 12 hours
schedule.every(12).hours.do(cleanup_old_data, engine=engine)


if __name__ == "__main__":
    print("Starting scheduled EMA crossover analysis...")
    while True:
        schedule.run_pending()
        time.sleep(1)
