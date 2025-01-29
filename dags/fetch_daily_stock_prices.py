import os
import json
import logging
import time  # <-- Import time for rate limiting
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import finnhub
from dag_config import POSTGRES_CONN_ID

# Correct path inside the Airflow container
RAW_DATA_DIR = "/opt/airflow/data/raw/"

# Get API key from environment
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise ValueError("FINNHUB_API_KEY is not set in the environment.")

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

# Finnhub Free Tier Limits
MAX_REQUESTS_PER_MINUTE = 60  # Limit based on free tier
BATCH_SIZE = 5  # Fetch stocks in batches
DELAY_BETWEEN_REQUESTS = 1  # 1-second delay between requests

def fetch_tech_companies_from_postgres():
    """Fetch the latest tech companies from PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tech_companies")
    companies = cursor.fetchall()
    cursor.close()
    conn.close()
    return [company[0] for company in companies]

def fetch_and_save_stock_data(symbol):
    """Fetch stock data from Finnhub API and save it as raw JSON."""
    logging.info(f"Fetching stock data for {symbol}")

    try:
        data = finnhub_client.quote(symbol)
        if not data:
            raise ValueError("Empty response received from Finnhub API.")

        # Create directory structure based on the current date
        date_path = datetime.utcnow().strftime('%Y/%m/%d')
        full_path = os.path.join(RAW_DATA_DIR, date_path)
        os.makedirs(full_path, exist_ok=True)

        # Save data to a file with a timestamp
        file_path = os.path.join(full_path, f"{symbol}_{datetime.utcnow().strftime('%Y%m%d')}.json")
        with open(file_path, "w") as f:
            json.dump(data, f)

        logging.info(f"✅ Data saved successfully: {file_path}")
    
    except Exception as e:
        logging.error(f"❌ Error fetching stock data for {symbol}: {e}")

def fetch_and_save_all_stock_data():
    """Fetch and save stock data for all tech companies."""
    companies = fetch_tech_companies_from_postgres()
    processed_count = 0  # Track how many requests have been made

    for company in companies:
        fetch_and_save_stock_data(company)
        processed_count += 1
        
        # Introduce delay after every request to avoid API rate limits
        if processed_count % 1 == 0:  # Every request
            logging.info(f"⏳ Processed {processed_count}/{len(companies)}. Sleeping for 1 sec to prevent hitting API rate limits.")
            time.sleep(1)  # Wait 1 second before next request

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_daily_stock_prices",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Run daily at midnight UTC
    catchup=False,
    tags=["finnhub", "data_ingestion"],
) as dag:
    
    fetch_stock_task = PythonOperator(
        task_id="fetch_and_save_all_stock_data",
        python_callable=fetch_and_save_all_stock_data,
    )

    fetch_stock_task
