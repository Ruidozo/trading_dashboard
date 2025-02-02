import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ✅ Get processed GCS bucket name from environment
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
if not GCS_BUCKET_PROCESSED:
    raise ValueError("🚨 GCS_BUCKET_PROCESSED is not set in the environment.")

def load_parquet_to_postgres():
    """Download the latest daily Parquet file from GCS and insert data into PostgreSQL."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    # ✅ Get today's date
    today = datetime.utcnow()
    year, month, day = today.strftime('%Y'), today.strftime('%m'), today.strftime('%d')

    # ✅ Path to the latest daily Parquet file
    monthly_folder = f"{year}/{month}/"
    parquet_filename = f"daily_stocks_{year}-{month}-{day}.parquet"
    gcs_parquet_path = f"{monthly_folder}{parquet_filename}"

    logging.info(f"🔍 Checking GCS path: gs://{GCS_BUCKET_PROCESSED}/{gcs_parquet_path}")

    # ✅ Download the Parquet file from GCS
    local_parquet_path = f"/tmp/{parquet_filename}"
    try:
        gcs_hook.download(bucket_name=GCS_BUCKET_PROCESSED, object_name=gcs_parquet_path, filename=local_parquet_path)
        logging.info(f"✅ Downloaded {gcs_parquet_path} from GCS.")
    except Exception as e:
        logging.error(f"❌ No file found for today: {e}")
        return

    # ✅ Read Parquet into DataFrame
    try:
        df = pd.read_parquet(local_parquet_path, engine="pyarrow")
        logging.info(f"✅ Loaded {df.shape[0]} rows from {parquet_filename}")
    except Exception as e:
        logging.error(f"❌ Failed to read Parquet file: {e}")
        return

    # ✅ Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    engine = pg_hook.get_sqlalchemy_engine()

    # ✅ Insert DataFrame into PostgreSQL
    try:
        with engine.begin() as conn:
            df.to_sql("daily_stock_data", conn, if_exists="append", index=False)
            logging.info(f"📥 Inserted {df.shape[0]} rows into PostgreSQL.")
    except Exception as e:
        logging.error(f"❌ Error inserting data into PostgreSQL: {e}")
        return

    # ✅ Cleanup local file
    os.remove(local_parquet_path)
    logging.info(f"🗑️ Deleted local file: {local_parquet_path}")

# ✅ Define Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_gcs_to_postgres",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Run daily at 3 AM UTC (after `process_gcs_json`)
    catchup=False,
    tags=["postgres", "data_load"],
) as dag:
    
    load_task = PythonOperator(
        task_id="load_parquet_data",
        python_callable=load_parquet_to_postgres,
    )

    load_task
