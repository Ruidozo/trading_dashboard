from yahooquery import Ticker

ticker = Ticker("AAPL")
news_data = ticker.news()  # ✅ Ensure the method is being executed

print(news_data)  # ✅ This should return a list of news items or an error
