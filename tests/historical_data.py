import requests
import datetime
import os


FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
symbol = "AAPL"  # Example: Apple

# Get the most recent trading day (Friday)
today = datetime.datetime.utcnow()
if today.weekday() == 5:  # If Saturday, go back 1 day
    trading_day = today - datetime.timedelta(days=1)
elif today.weekday() == 6:  # If Sunday, go back 2 days
    trading_day = today - datetime.timedelta(days=2)
else:
    trading_day = today

# Convert to Unix timestamp
timestamp = int(trading_day.timestamp())

# Fetch candlestick data for a recent trading day
url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D&from={timestamp-86400}&to={timestamp}&token={FINNHUB_API_KEY}"
response = requests.get(url)
data = response.json()

print(data)  # Check if 'v' (volume) contains data

