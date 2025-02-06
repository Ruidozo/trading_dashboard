import os
import time
import logging
import requests
import json
import pandas as pd
import nltk
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# ✅ Environment Variables
API_KEY = os.getenv("FINNHUB_API_KEY")  # Ensure this is set
POSTGRES_CONN_ID = "project_postgres"
STAGING_TABLE = "staging_company_news"

# Ensure required NLTK data is available
nltk.download('vader_lexicon')

# ✅ Define Date Range for Fetching News
today = datetime.utcnow().date()
yesterday = today - timedelta(days=1)

# ✅ Finnhub General Market News Endpoint (to filter relevant companies)
GENERAL_NEWS_URL = f"https://finnhub.io/api/v1/news?category=general&token={API_KEY}"

# ✅ Function to Fetch Company Symbols from PostgreSQL
def fetch_company_symbols():
    """Fetch symbols from tech_companies table."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tech_companies")
    companies = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return companies

# ✅ Function to Get Relevant Companies in Recent News
def get_companies_with_news():
    """Fetches recent general market news and extracts relevant company symbols."""
    try:
        response = requests.get(GENERAL_NEWS_URL)
        if response.status_code == 200:
            general_news = response.json()
            mentioned_companies = set()

            logging.info(f"📢 Raw general news response: {json.dumps(general_news, indent=2)}")  # Debugging full response

            for article in general_news:
                related_tickers = article.get("related", None)

                if not related_tickers:
                    logging.warning(f"⚠️ No related tickers found in article: {article['headline']}")
                    continue  # Skip this article if no tickers are related

                if isinstance(related_tickers, str):
                    related_tickers = related_tickers.split(",")  # ✅ Split CSV tickers
                
                elif isinstance(related_tickers, list):
                    related_tickers = [str(ticker).strip() for ticker in related_tickers]  # ✅ Handle list format

                mentioned_companies.update(related_tickers)

            logging.info(f"✅ Companies mentioned in recent news: {mentioned_companies}")

            if not mentioned_companies:
                logging.warning("⚠️ No companies extracted from Finnhub general news. Possible API issue.")

            return mentioned_companies

        else:
            logging.warning(f"⚠️ Failed to fetch general news: {response.text}")
            return set()

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Failed to fetch general news: {e}")
        return set()

# ✅ Function to Fetch News from Finnhub
# ✅ Function to Fetch News from Finnhub
def fetch_company_news():
    """Fetches news for all companies and stores in PostgreSQL."""
    symbols = fetch_company_symbols()
    batch_size = 10  # ✅ Process in batches to reduce API calls
    all_news = []

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        for symbol in batch:
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={yesterday}&to={today}&token={API_KEY}"
            retries = 3  # ✅ Retry up to 3 times if rate limited

            for attempt in range(retries):
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        news_data = response.json()
                        logging.info(f"✅ Received {len(news_data)} articles for {symbol}")

                        for news in news_data:
                            all_news.append({
                                "symbol": symbol,
                                "date": datetime.utcfromtimestamp(news["datetime"]).date(),
                                "headline": news.get("headline", ""),
                                "summary": news.get("summary", ""),
                                "source": news.get("source", ""),
                                "url": news.get("url", ""),
                            })

                        time.sleep(1)  # ✅ Prevent hitting rate limit
                        break

                    elif response.status_code == 429:
                        wait_time = (attempt + 1) * 5  # Exponential backoff
                        logging.warning(f"⚠️ Rate limit hit. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)

                    else:
                        logging.error(f"❌ Error fetching news for {symbol}: {response.text}")
                        break

                except requests.exceptions.RequestException as e:
                    logging.error(f"❌ Request failed for {symbol}: {e}")
                    break  # No more retries

    # ✅ Convert Data to DataFrame
    if all_news:
        df = pd.DataFrame(all_news)
        save_news_to_postgres(df)
    else:
        logging.info("⚠️ No news articles fetched.")

# ✅ Function to Store News in PostgreSQL
def save_news_to_postgres(df):
    """Saves news to PostgreSQL staging table (replaces daily)."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    # ✅ Replace `staging_company_news` daily
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE {STAGING_TABLE} RESTART IDENTITY;")  # ✅ Ensures only the latest news is stored
        df.to_sql(STAGING_TABLE, conn, if_exists="append", index=False)
        logging.info(f"✅ Inserted {df.shape[0]} rows into {STAGING_TABLE}")

def transform_and_store_news():
    # Connect to PostgreSQL using Airflow's PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='project_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Fetch data from staging table
        cursor.execute("SELECT symbol, date, headline, summary, source, url FROM staging_company_news;")
        news_records = cursor.fetchall()
        
        # Convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(news_records, columns=columns)
        df.rename(columns={'date': 'news_date'}, inplace=True)

        print(f"🛠️ Available columns in DataFrame: {df.columns.tolist()}")
        
        # Transform the data (basic transformation example)
        df['news_date'] = pd.to_datetime(df['news_date'])
        df = df.sort_values(by='news_date', ascending=False)
        
        # Compute sentiment scores
        sia = SentimentIntensityAnalyzer()
        df['sentiment_score'] = df.apply(lambda row: sia.polarity_scores(str(row['headline'] or '') + ' ' + str(row['summary'] or ''))['compound'], axis=1)
        
        
        # Insert transformed data into daily_company_news table (partitioned)
        insert_query = """
        INSERT INTO daily_company_news (symbol, news_date, headline, summary, source, url, sentiment_score, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, headline, news_date, date) DO NOTHING;

        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, (row['symbol'], row['news_date'], row['headline'], row['summary'], row['source'], row['url'], row['sentiment_score'], row['partition_date']))
        
        conn.commit()
    except Exception as e:
        print(f"Error during transformation: {e}")
    finally:
        cursor.close()
        conn.close()

def fetch_transform_news_taskgroup(dag):
    with TaskGroup("fetch_transform_news", dag=dag) as fetch_transform_news:
        fetch_news_task = PythonOperator(task_id="fetch_company_news", python_callable=fetch_company_news)
        transform_news_task = PythonOperator(task_id="transform_and_store_news", python_callable=transform_and_store_news)
        fetch_news_task >> transform_news_task
    return fetch_transform_news