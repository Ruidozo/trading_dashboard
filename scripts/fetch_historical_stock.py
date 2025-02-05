import os
import time
import requests
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import yfinance as yf

# ✅ Load environment variables from .env file
load_dotenv()

# ✅ PostgreSQL Connection Details from .env
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

print("🔍 Attempting to connect to PostgreSQL with:")
print(f"DB_NAME: {DB_CONFIG['dbname']}")
print(f"DB_USER: {DB_CONFIG['user']}")
print(f"DB_HOST: {DB_CONFIG['host']}")
print(f"DB_PORT: {DB_CONFIG['port']}")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")

# ✅ Define Historical Data Range
START_DATE = "2023-01-01"
END_DATE = datetime.utcnow().strftime('%Y-%m-%d')

def get_tech_companies():
    """Fetch tech company symbols and names from PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT symbol, name FROM tech_companies;")  # Fetch both symbol & name
    companies = [{"symbol": row[0], "company_name": row[1]} for row in cur.fetchall()]
    conn.close()
    return companies

def fetch_historical_data(symbol):
    """Fetch historical stock data for a given symbol using Yahoo Finance."""
    print(f"📡 Fetching historical data for {symbol} from Yahoo Finance...")
    
    try:
        stock = yf.Ticker(symbol)
        df = stock.history(start="2025-01-01", end="2025-01-31")  # Fetch data from 01-01-2025 to 31-01-2025

        if df.empty:
            print(f"⚠️ No data found for {symbol}. Skipping.")
            return None

        # Reset index to get `Date` as a column
        df.reset_index(inplace=True)

        # Rename columns to match database schema
        df.rename(columns={
            "Date": "trade_date",
            "Open": "opening_price",
            "High": "highest_price",
            "Low": "lowest_price",
            "Close": "closing_price",
            "Volume": "traded_volume",
        }, inplace=True)

        # Keep only necessary columns
        df = df[["trade_date", "opening_price", "highest_price", "lowest_price", "closing_price", "traded_volume"]]

        return df

    except Exception as e:
        print(f"❌ Failed to fetch data for {symbol}: {e}")
        return None

def store_data_in_postgres(df, symbol, company_name):
    """Insert stock data into PostgreSQL, avoiding duplicates."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO stock_price_history (symbol, company_name, trade_date, opening_price, highest_price, lowest_price, closing_price, traded_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) DO NOTHING;
        """, (symbol, company_name, row["trade_date"], row["opening_price"], row["highest_price"], row["lowest_price"], row["closing_price"], row["traded_volume"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Stored {len(df)} records for {symbol} ({company_name}) in PostgreSQL.")

def main():
    """Main function to fetch historical stock data and store it."""
    companies = get_tech_companies()
    for company in companies:
        symbol = company["symbol"]
        company_name = company["company_name"]
        
        print(f"📡 Fetching data for {symbol} ({company_name})...")
        df = fetch_historical_data(symbol)

        if df is not None:
            store_data_in_postgres(df, symbol, company_name)
        
        time.sleep(2)  # Prevent API rate limits

    print("🚀 Historical data fetching complete!")

if __name__ == "__main__":
    main()
