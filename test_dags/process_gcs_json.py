import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # ✅ Import Trigger Operator

# ✅ Environment variables for both buckets
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")  # Raw JSON bucket
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")  # Processed Parquet bucket

if not GCS_BUCKET_NAME or not GCS_BUCKET_PROCESSED:
    raise ValueError("🚨 GCS_BUCKET_NAME or GCS_BUCKET_PROCESSED is not set in the environment.")

def list_gcs_files():
    """List JSON files in GCS for today's date (dynamic path)."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    
    today = datetime.utcnow().strftime('%Y/%m/%d')  # Match upload structure
    prefix = f"{today}/"  # Example: "2025/01/30/"
    
    logging.info(f"🔍 Checking GCS path: gs://{GCS_BUCKET_NAME}/{prefix}")
    files = gcs_hook.list(bucket_name=GCS_BUCKET_NAME, prefix=prefix)

    if not files:
        logging.warning(f"⚠️ No new files found in gs://{GCS_BUCKET_NAME}/{prefix}")
        return []

    logging.info(f"✅ Found {len(files)} files in GCS for {today}.")
    return files

def process_json_data():
    """Fetch JSON files from GCS, add stock symbol, and save as a single Parquet file inside a monthly folder."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    files = list_gcs_files()
    
    if not files:
        return
    
    all_data = []  # ✅ Store multiple stocks in a single list
    
    for file in files:
        try:
            # ✅ Extract stock symbol from filename
            filename = file.split("/")[-1]  # Extract actual filename
            stock_symbol = filename.split("_")[0]  # Example: "AAPL_2025-01-30.json" → "AAPL"
            
            # ✅ Download JSON content
            json_data = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=file)
            json_content = json.loads(json_data.decode('utf-8'))  # Decode bytes to string

            # ✅ Convert JSON to DataFrame and assign symbol
            df = pd.DataFrame([json_content])
            df["symbol"] = stock_symbol  # ✅ Assign stock symbol column
            df["date"] = datetime.utcnow().strftime('%Y-%m-%d')  # ✅ Add date column
            
            all_data.append(df)

            logging.info(f"📊 Processed {file}: {df.shape}")

        except Exception as e:
            logging.error(f"❌ Error processing {file}: {e}")

    # ✅ Combine all DataFrames into one daily file
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        logging.info(f"✅ Final DataFrame Shape: {final_df.shape}")

        # ✅ Save as a single Parquet file per day in the monthly folder
        save_parquet_to_gcs(final_df)

def save_parquet_to_gcs(df):
    """Save the daily DataFrame as a single Parquet file and upload to GCS inside a monthly folder."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    today = datetime.utcnow()
    year, month, day = today.strftime('%Y'), today.strftime('%m'), today.strftime('%d')
    
    # ✅ New Path Format: "2025/01/daily_stocks_2025-01-30.parquet"
    monthly_folder = f"{year}/{month}/"
    parquet_filename = f"daily_stocks_{year}-{month}-{day}.parquet"
    
    # ✅ Local Path
    local_parquet_path = f"/tmp/{parquet_filename}"  
    df.to_parquet(local_parquet_path, index=False)

    # ✅ Upload Path in GCS
    gcs_parquet_path = f"{monthly_folder}{parquet_filename}"
    gcs_hook.upload(
        bucket_name=GCS_BUCKET_PROCESSED,  # ✅ Upload to the new processed bucket
        object_name=gcs_parquet_path,
        filename=local_parquet_path
    )

    logging.info(f"✅ Parquet file uploaded: gs://{GCS_BUCKET_PROCESSED}/{gcs_parquet_path}")

# ✅ Define Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_gcs_json",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Run daily at 2 AM UTC
    catchup=False,
    tags=["gcs", "data_processing"],
) as dag:
    
    process_task = PythonOperator(
        task_id="process_json_data",
        python_callable=process_json_data,
    )

    trigger_load_task = TriggerDagRunOperator(
        task_id="trigger_load_gcs_to_postgres",
        trigger_dag_id="load_gcs_to_postgres",  # ✅ Triggers the next DAG
        wait_for_completion=False,
    )

    # ✅ Ensure the DAG sequence
    process_task >> trigger_load_task
