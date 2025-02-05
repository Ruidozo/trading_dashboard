import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import finnhub

# Define a function to test the Finnhub API
def test_finnhub_api():
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is not set in the environment.")
    
    client = finnhub.Client(api_key=api_key)
    try:
        # Test API by fetching data for AAPL (Apple stock)
        response = client.quote("AAPL")
        print("Finnhub API response:", response)
        if not response or "c" not in response:
            raise ValueError("API response invalid or empty.")
    except Exception as e:
        raise RuntimeError(f"Error testing Finnhub API: {e}")

# Create the Airflow DAG
with DAG(
    dag_id="test_finnhub_api",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    tags=["test"],
    catchup=False,
) as dag:
    test_api_task = PythonOperator(
        task_id="test_finnhub_api_task",
        python_callable=test_finnhub_api,
    )
