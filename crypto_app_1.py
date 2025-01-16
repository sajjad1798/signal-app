import pandas as pd
from ta.trend import EMAIndicator
import requests
import time
import schedule
from binance.client import Client

# Binance API configuration
API_KEY = "your_api_key"
API_SECRET = "your_api_secret"
client = Client(API_KEY, API_SECRET)

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "7717260550:AAEfClasfGBAS6zmQj0waLUTQqQeyECyr1s"
TELEGRAM_CHAT_ID = "1638869534"

# Configuration
INTERVAL = Client.KLINE_INTERVAL_5MINUTE  # Directly fetching 5-minute interval candles
HISTORY_LIMIT = 1000  # Ensure we fetch enough candles for EMA calculation


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


# Fetch historical price data for a single symbol from Binance
def fetch_historical_data(symbol, interval=INTERVAL, limit=HISTORY_LIMIT):
    try:
        klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(
            klines,
            columns=[
                "timestamp",
                "o",
                "h",
                "l",
                "c",
                "v",
                "close_time",
                "quote_asset_volume",
                "trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
        # Convert relevant columns to numeric
        for col in ["o", "h", "l", "c", "v"]:
            df[col] = pd.to_numeric(df[col])
        # Convert timestamp to datetime
        df["E"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df[["E", "o", "h", "l", "c", "v"]]
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()


# Analyze a single coin for EMA crossovers
def analyze_coin_for_crossover(symbol):
    df = fetch_historical_data(symbol)

    if df.empty:
        print(f"{symbol}: No data available.")
        return None

    # Ensure enough data for EMA calculations
    if len(df) < 200:
        print(f"{symbol}: Not enough data for EMA calculations.")
        return None

    close_prices = df["c"]

    # Calculate EMAs
    df["EMA_50"] = EMAIndicator(close=close_prices, window=50).ema_indicator()
    df["EMA_200"] = EMAIndicator(close=close_prices, window=200).ema_indicator()

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    print("latest", latest)
    print("prev", prev)

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


# Fetch all USDT trading pairs from Binance
def get_usdt_pairs():
    try:
        symbols = client.get_exchange_info()["symbols"]
        return [s["symbol"] for s in symbols if s["symbol"].endswith("USDT")]
    except Exception as e:
        print(f"Error fetching trading pairs: {e}")
        return []


# Analyze all coins for crossover
def analyze_all_coins_for_crossover():
    tokens = get_usdt_pairs()
    results = []
    for token in tokens:
        result = analyze_coin_for_crossover(token)
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


# Schedule the analysis every 5 minutes
schedule.every(1).minute.do(run_crossover_analysis)

if __name__ == "__main__":
    print("Starting scheduled EMA crossover analysis...")
    while True:
        schedule.run_pending()
        time.sleep(1)
