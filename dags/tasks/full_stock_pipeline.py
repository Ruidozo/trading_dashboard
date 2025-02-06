import os
import json
import logging
import time
import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as yf

# ✅ Environment Variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
POSTGRES_CONN_ID = "project_postgres"

# ✅ Define Functions (Keep logic unchanged)
def fetch_and_save_stock_data():
    """Fetch stock data from Yahoo Finance and save as JSON."""
    logging.info("🔄 Fetching stock data...")
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tech_companies")
    companies = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    for symbol in companies:
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1d", interval="1d")

            if hist.empty:
                logging.warning(f"⚠️ No data for {symbol}. Skipping.")
                continue

            hist.reset_index(inplace=True)
            hist.rename(columns={
                "Date": "trade_date",
                "Open": "opening_price",
                "High": "highest_price",
                "Low": "lowest_price",
                "Close": "closing_price",
                "Volume": "traded_volume",
            }, inplace=True)

            data = hist.to_dict(orient="records")
            data = [{**row, "trade_date": row["trade_date"].strftime("%Y-%m-%d")} for row in data]

            file_path = f"/opt/airflow/data/raw/{symbol}_{datetime.utcnow().strftime('%Y%m%d')}.json"
            with open(file_path, "w") as f:
                json.dump(data, f)

            logging.info(f"✅ Saved data for {symbol}")
            time.sleep(0)

        except Exception as e:
            logging.error(f"❌ Error fetching stock data for {symbol}: {e}")

    logging.info("🚀 Stock data fetch complete.")

def upload_json_to_gcs():
    """Upload JSON files to GCS."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    raw_data_dir = "/opt/airflow/data/raw/"

    for file in os.listdir(raw_data_dir):
        if file.endswith(".json"):
            local_file_path = os.path.join(raw_data_dir, file)
            try:
                gcs_hook.upload(
                    bucket_name=GCS_BUCKET_NAME,
                    object_name=f"raw/{file}",
                    filename=local_file_path
                )
                os.remove(local_file_path)
                logging.info(f"✅ Uploaded {file} to GCS")
            except Exception as e:
                logging.error(f"❌ Failed to upload {file}: {e}")

def process_json_to_parquet():
    """Convert JSON to Parquet and upload to GCS."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    today = datetime.utcnow().strftime('%Y/%m/%d')
    files = gcs_hook.list(bucket_name=GCS_BUCKET_NAME, prefix=f"raw/")

    if not files:
        logging.warning("⚠️ No JSON files found.")
        return

    all_data = []
    for file in files:
        try:
            json_data = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=file)
            json_content = json.loads(json_data.decode('utf-8'))
            if not isinstance(json_content, list):
                continue
            all_data.extend(json_content)
        except Exception as e:
            logging.error(f"❌ Error processing {file}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        parquet_filename = f"processed/daily_stocks_{datetime.utcnow().strftime('%Y-%m-%d')}.parquet"
        local_parquet_path = f"/tmp/daily_stocks.parquet"
        df.to_parquet(local_parquet_path, index=False)

        gcs_hook.upload(bucket_name=GCS_BUCKET_PROCESSED, object_name=parquet_filename, filename=local_parquet_path)
        os.remove(local_parquet_path)
        logging.info(f"✅ Parquet uploaded: {parquet_filename}")

def load_parquet_to_postgres():
    """Load processed Parquet file into PostgreSQL."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    parquet_filename = f"processed/daily_stocks_{datetime.utcnow().strftime('%Y-%m-%d')}.parquet"
    local_parquet_path = f"/tmp/daily_stocks.parquet"

    try:
        gcs_hook.download(bucket_name=GCS_BUCKET_PROCESSED, object_name=parquet_filename, filename=local_parquet_path)
        df = pd.read_parquet(local_parquet_path, engine="pyarrow")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql("staging_stock_data", engine, if_exists="replace", index=False)
        logging.info("✅ Staging table updated")
    except Exception as e:
        logging.error(f"❌ Error loading Parquet to PostgreSQL: {e}")

def update_stock_price_history():
    """Merge staging data into stock_price_history."""
    sql_query = """
    INSERT INTO stock_price_history (...) SELECT ... FROM staging_stock_data
    ON CONFLICT (symbol, trade_date) DO UPDATE SET ...;
    """
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    pg_hook.run(sql_query)
    logging.info("✅ Stock price history updated.")

def detect_trading_patterns():
    """Detect and store trading patterns."""
    sql_query = """
    INSERT INTO trading_patterns (...) SELECT ... FROM stock_price_history
    ON CONFLICT (symbol, trade_date, pattern) DO NOTHING;
    """
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    pg_hook.run(sql_query)
    logging.info("✅ Trading patterns detected.")

# ✅ Convert to TaskGroup Function
def full_stock_pipeline_taskgroup(dag):
    with TaskGroup("full_stock_pipeline", dag=dag) as full_stock_pipeline:
        fetch_stock_task = PythonOperator(task_id="fetch_stock_data", python_callable=fetch_and_save_stock_data)
        upload_json_task = PythonOperator(task_id="upload_json_to_gcs", python_callable=upload_json_to_gcs)
        process_parquet_task = PythonOperator(task_id="process_json_to_parquet", python_callable=process_json_to_parquet)
        load_postgres_task = PythonOperator(task_id="load_parquet_to_postgres", python_callable=load_parquet_to_postgres)
        update_stock_task = PythonOperator(task_id="update_stock_price_history", python_callable=update_stock_price_history)
        detect_patterns_task = PythonOperator(task_id="detect_trading_patterns", python_callable=detect_trading_patterns)

        # ✅ Maintain Dependencies
        fetch_stock_task >> upload_json_task >> process_parquet_task >> load_postgres_task >> update_stock_task >> detect_patterns_task

    return full_stock_pipeline
