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

# ✅ Get PostgreSQL connection ID from environment
POSTGRES_CONN_ID = "project_postgres"

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
        expected_columns = ["symbol", "date", "c", "d", "dp", "h", "l", "o", "pc", "t"]
        df = df[expected_columns]
        df["date"] = pd.to_datetime(df["date"]).dt.date  # Ensure proper date format

        # ✅ Convert types to match PostgreSQL schema
        df = df.astype({
            "c": float, "h": float, "l": float, "o": float, "pc": float,
            "t": int  # Ensure `t` is stored as BIGINT in PostgreSQL
        })

        # ✅ Insert data into `staging_stock_data` (Replace existing daily batch)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            df.to_sql("staging_stock_data", conn, if_exists="replace", index=False)

        logging.info(f"📥 Replaced staging table with {df.shape[0]} rows.")


    except Exception as e:
        logging.error(f"❌ Error loading Parquet to PostgreSQL: {e}")

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
