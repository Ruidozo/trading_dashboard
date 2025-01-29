import os
import json
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import finnhub

# Correct path inside the Airflow container
RAW_DATA_DIR = "/opt/airflow/data/raw/"

# Ensure the directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Get API key from environment
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    raise ValueError("FINNHUB_API_KEY is not set in the environment.")

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

def fetch_and_save_stock_data():
    """Fetch stock data from Finnhub API and save it as raw JSON."""
    stock_symbol = "AAPL"  # Modify this if needed
    logging.info(f"Fetching stock data for {stock_symbol}")

    try:
        data = finnhub_client.quote(stock_symbol)
        if not data:
            raise ValueError("Empty response received from Finnhub API.")

        # Save data to a file with a timestamp
        file_path = os.path.join(RAW_DATA_DIR, f"{stock_symbol}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json")
        with open(file_path, "w") as f:
            json.dump(data, f)

        logging.info(f"Data saved successfully: {file_path}")
    
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        raise

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="finnhub_daily_ingestion",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Run daily at midnight UTC
    catchup=False,
    tags=["finnhub", "data_ingestion"],
) as dag:
    
    fetch_stock_task = PythonOperator(
        task_id="fetch_and_save_stock_data",
        python_callable=fetch_and_save_stock_data,
    )

    fetch_stock_task
