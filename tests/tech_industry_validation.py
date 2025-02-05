import finnhub
import os

api_key = os.getenv("FINNHUB_API_KEY")
client = finnhub.Client(api_key=api_key)

# Fetch all US stocks (without industry info)
companies = client.stock_symbols('US')

# Fetch industry info for a few stocks to test
sample_symbols = ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META']
industries = {}

for symbol in sample_symbols:
    profile = client.company_profile2(symbol=symbol)
    industries[symbol] = profile.get('finnhubIndustry', 'Unknown')

print("✅ Industry classifications from Finnhub API:")
for symbol, industry in industries.items():
    print(f"{symbol}: {industry}")
