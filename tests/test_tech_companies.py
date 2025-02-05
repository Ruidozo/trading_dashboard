import finnhub
import os

api_key = os.getenv("FINNHUB_API_KEY")
client = finnhub.Client(api_key=api_key)

# Fetch all US stock symbols
companies = client.stock_symbols('US')

# Check if AAPL is present
aapl_company = next((c for c in companies if c['symbol'] == 'AAPL'), None)

if aapl_company:
    print("✅ AAPL is included in the Finnhub response:", aapl_company)
else:
    print("❌ AAPL is NOT included in the Finnhub response.")
