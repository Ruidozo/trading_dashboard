import os
import json
import logging
import time
import pandas as pd
import traceback
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import finnhub

# ✅ Environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
POSTGRES_CONN_ID = "project_postgres"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not all([GCS_BUCKET_NAME, GCS_BUCKET_PROCESSED, FINNHUB_API_KEY]):
    raise ValueError("🚨 One or more required environment variables are missing.")

# ✅ Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

# ✅ Define local raw data storage path
RAW_DATA_DIR = "/opt/airflow/data/raw/"

# ✅ Function to fetch tech company symbols from PostgreSQL
def fetch_tech_companies():
    """Fetches tech company symbols from PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tech_companies")
    companies = cursor.fetchall()
    cursor.close()
    conn.close()
    return [company[0] for company in companies]

def fetch_and_save_stock_data():
    """Fetches stock data from Finnhub and saves it as JSON."""
    logging.info("🔄 Starting stock data fetch...")
    companies = fetch_tech_companies()

    for symbol in companies:
        try:
            data = finnhub_client.quote(symbol)

            if not data or "c" not in data:  
                logging.warning(f"⚠️ No valid data received for {symbol}. Skipping.")
                continue

            # ✅ Ensure `v` is included
            if "v" not in data:
                data["v"] = 0  # Default to 0 if missing

            date_path = datetime.utcnow().strftime('%Y/%m/%d')
            full_path = os.path.join(RAW_DATA_DIR, date_path)
            os.makedirs(full_path, exist_ok=True)

            file_path = os.path.join(full_path, f"{symbol}_{datetime.utcnow().strftime('%Y%m%d')}.json")
            with open(file_path, "w") as f:
                json.dump(data, f)

            logging.info(f"✅ Data saved: {file_path}")

            time.sleep(2)  

        except Exception as e:
            logging.error(f"❌ Error fetching stock data for {symbol}: {e}\n{traceback.format_exc()}")

    logging.info("🚀 Finished fetching stock data.")


def upload_json_to_gcs():
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    for root, _, files in os.walk(RAW_DATA_DIR):
        for file in files:
            if file.endswith(".json"):
                local_file_path = os.path.join(root, file)
                gcs_file_path = os.path.relpath(local_file_path, RAW_DATA_DIR)

                try:
                    gcs_hook.upload(
                        bucket_name=GCS_BUCKET_NAME,
                        object_name=gcs_file_path,
                        filename=local_file_path
                    )
                    os.remove(local_file_path)
                    logging.info(f"✅ Uploaded and deleted {local_file_path}")

                except Exception as e:
                    logging.error(f"❌ Failed to upload {local_file_path}: {e}")

def process_json_to_parquet():
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    today = datetime.utcnow().strftime('%Y/%m/%d')
    files = gcs_hook.list(bucket_name=GCS_BUCKET_NAME, prefix=f"{today}/")

    if not files:
        logging.warning(f"⚠️ No JSON files found in GCS for {today}")
        return

    all_data = []
    for file in files:
        try:
            json_data = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=file)
            json_content = json.loads(json_data.decode('utf-8'))

            symbol = file.split("/")[-1].split("_")[0]
            json_content["symbol"] = symbol
            json_content["date"] = datetime.utcnow().strftime('%Y-%m-%d')

            all_data.append(json_content)

        except Exception as e:
            logging.error(f"❌ Error processing {file}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        year_month = datetime.utcnow().strftime('%Y/%m')
        parquet_filename = f"daily_stocks_{datetime.utcnow().strftime('%Y-%m-%d')}.parquet"
        local_parquet_path = f"/tmp/{parquet_filename}"

        df.to_parquet(local_parquet_path, index=False)
        gcs_hook.upload(
            bucket_name=GCS_BUCKET_PROCESSED,
            object_name=f"{year_month}/{parquet_filename}",
            filename=local_parquet_path
        )
        os.remove(local_parquet_path)
        logging.info(f"✅ Parquet saved and uploaded: {parquet_filename}")

def load_parquet_to_postgres():
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    today = datetime.utcnow()
    year_month = today.strftime('%Y/%m')
    parquet_filename = f"daily_stocks_{today.strftime('%Y-%m-%d')}.parquet"
    gcs_parquet_path = f"{year_month}/{parquet_filename}"
    local_parquet_path = f"/tmp/{parquet_filename}"

    try:
        # ✅ Download Parquet from GCS
        gcs_hook.download(bucket_name=GCS_BUCKET_PROCESSED, object_name=gcs_parquet_path, filename=local_parquet_path)
        df = pd.read_parquet(local_parquet_path, engine="pyarrow")

        # ✅ Ensure column order & rename for PostgreSQL compatibility
        expected_columns = ["symbol", "date", "c", "d", "dp", "h", "l", "o", "pc", "t", "v"]
        df = df[expected_columns]
        df["date"] = pd.to_datetime(df["date"]).dt.date  # Ensure proper date format

        # ✅ Convert types to match PostgreSQL schema
        df = df.astype({
            "c": float, "h": float, "l": float, "o": float, "pc": float,
            "t": int, "v":int  
        })

        # ✅ Insert data into `staging_stock_data` (Replace existing daily batch)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            df.to_sql("staging_stock_data", conn, if_exists="replace", index=False)

        logging.info(f"📥 Replaced staging table with {df.shape[0]} rows.")

        os.remove(local_parquet_path)

    except Exception as e:
        logging.error(f"❌ Error loading Parquet to PostgreSQL: {e}")



# ✅ Define DAG with constraints
default_args = {
    "owner": "airflow",
    "depends_on_past": False,  
    "start_date": datetime(2025, 1, 2),
    "retries": 3,  
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="full_stock_pipeline",
    default_args=default_args,
    schedule_interval="10 0 * * 1-5",  # ✅ Runs daily at 00:10 UTC
    catchup=False,
    max_active_runs=1,
    tags=["finnhub", "data_ingestion", "gcs", "postgres"],
) as dag:


    fetch_stock_task = PythonOperator(
        task_id="fetch_and_save_stock_data",
        python_callable=fetch_and_save_stock_data,
    )

    upload_json_task = PythonOperator(
        task_id="upload_json_to_gcs",
        python_callable=upload_json_to_gcs,
    )

    process_parquet_task = PythonOperator(
        task_id="process_json_to_parquet",
        python_callable=process_json_to_parquet,
    )

    load_postgres_task = PythonOperator(
        task_id="load_parquet_to_postgres",
        python_callable=load_parquet_to_postgres,
    )

    # ✅ Define the DAG sequence
    fetch_stock_task >> upload_json_task >> process_parquet_task >> load_postgres_task
