import os
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

# ✅ Environment Variables
API_KEY = os.getenv("FINNHUB_API_KEY")  # Make sure this is set!
POSTGRES_CONN_ID = "project_postgres"
STAGING_TABLE = "staging_company_news"
FINAL_TABLE = "daily_company_news"

# ✅ Define Date Range for Fetching News (Yesterday to Today)
today = datetime.utcnow().date()
yesterday = today - timedelta(days=1)

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
    """Saves news to PostgreSQL staging table and merges into final table."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    # ✅ Replace `staging_company_news`
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE {STAGING_TABLE} RESTART IDENTITY;")
        df.to_sql(STAGING_TABLE, conn, if_exists="append", index=False)
        logging.info(f"✅ Inserted {df.shape[0]} rows into {STAGING_TABLE}")

    # ✅ Merge into `daily_company_news`
    with engine.begin() as conn:
        merge_sql = f"""
        INSERT INTO {FINAL_TABLE} (symbol, date, headline, summary, source, url)
        SELECT s.symbol, s.date, s.headline, s.summary, s.source, s.url
        FROM {STAGING_TABLE} s
        ON CONFLICT (symbol, date, url) DO NOTHING;
        """
        conn.execute(merge_sql)
        logging.info(f"✅ Merged data into {FINAL_TABLE}")

# ✅ Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 2),
    "retries": 1,
}

with DAG(
    dag_id="fetch_company_news",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # ✅ Runs every day at 6 AM UTC
    catchup=False,
    tags=["finnhub", "news", "postgres"],
) as dag:

    fetch_news_task = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_company_news,
    )

fetch_news_task
