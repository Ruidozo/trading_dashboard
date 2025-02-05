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
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import finnhub

# ✅ Environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
POSTGRES_CONN_ID = "project_postgres"
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not all([GCS_BUCKET_NAME, GCS_BUCKET_PROCESSED, FINNHUB_API_KEY]):
    raise ValueError("🚨 One or more required environment variables are missing.")

# Configure logging
log = logging.getLogger(__name__)

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

# ✅ Fetch and Save Stock Data
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
                data["v"] = 0

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

# ✅ Upload JSON to GCS
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

# ✅ Process JSON to Parquet
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

# ✅ Load Parquet to PostgreSQL
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

# ✅ Update Stock Price History
def update_stock_price_history():
    """Merge `staging_stock_data` into partitioned `stock_price_history`."""
    log.info("🔄 Updating partitioned stock_price_history with latest stock data...")

    sql_query = """
    INSERT INTO stock_price_history (
        trade_date, market_cap_rank, company_name, country, symbol, 
        opening_price, highest_price, lowest_price, closing_price, 
        previous_closing_price, traded_volume, unix_timestamp
    )
    SELECT 
        s.date AS trade_date,
        t.rank AS market_cap_rank,
        t.name AS company_name,
        t.country,
        s.symbol,
        s.o AS opening_price,
        s.h AS highest_price,
        s.l AS lowest_price,
        s.c AS closing_price,
        s.pc AS previous_closing_price,
        COALESCE(s.v, 0) AS traded_volume,
        s.t AS unix_timestamp
    FROM staging_stock_data s
    LEFT JOIN tech_companies t ON s.symbol = t.symbol
    ON CONFLICT (symbol, trade_date) 
    DO UPDATE SET 
        market_cap_rank = EXCLUDED.market_cap_rank,
        company_name = EXCLUDED.company_name,
        country = EXCLUDED.country,
        opening_price = EXCLUDED.opening_price,
        highest_price = EXCLUDED.highest_price,
        lowest_price = EXCLUDED.lowest_price,
        closing_price = EXCLUDED.closing_price,
        previous_closing_price = EXCLUDED.previous_closing_price,
        traded_volume = EXCLUDED.traded_volume,
        unix_timestamp = EXCLUDED.unix_timestamp;
    """

    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    pg_hook.run(sql_query)
    
    log.info("✅ Partitioned stock_price_history updated successfully!")

# ✅ Detect Trading Patterns
def detect_trading_patterns():
    """Detect trading patterns (trend-based and moving averages) and store them in `trading_patterns`."""
    log.info("🔍 Running trading pattern detection...")

    sql_query = """
    -- Detect Bullish & Bearish Trends
        INSERT INTO trading_patterns (symbol, trade_date, pattern_category, pattern)
        SELECT symbol, trade_date, 'Trend', pattern
        FROM (
            SELECT symbol, trade_date,
                CASE 
                        WHEN closing_price > LAG(closing_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                        AND LAG(closing_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date) > LAG(closing_price, 2) OVER (PARTITION BY symbol ORDER BY trade_date)
                        THEN 'Bullish'
                        WHEN closing_price < LAG(closing_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
                        AND LAG(closing_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date) < LAG(closing_price, 2) OVER (PARTITION BY symbol ORDER BY trade_date)
                        THEN 'Bearish'
                        ELSE NULL
                END AS pattern
            FROM stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '200 days'
            AND closing_price IS NOT NULL  -- ✅ Ensure closing_price exists
        ) subquery
        WHERE pattern IS NOT NULL  -- ✅ Prevent NULL patterns
        ON CONFLICT (symbol, trade_date, pattern) DO NOTHING;

        -- Detect Golden Cross & Death Cross (Moving Averages)
        WITH moving_averages AS (
            SELECT symbol, trade_date, 
                AVG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma_50,
                AVG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma_200
            FROM stock_price_history
        )
        INSERT INTO trading_patterns (symbol, trade_date, pattern_category, pattern)
        SELECT symbol, trade_date, 'Moving_Averages', pattern
        FROM (
            SELECT symbol, trade_date,
                CASE 
                    WHEN ma_50 > ma_200 THEN 'Golden Cross'
                    WHEN ma_50 < ma_200 THEN 'Death Cross'
                    ELSE NULL
                END AS pattern
            FROM moving_averages
            WHERE ma_50 IS NOT NULL AND ma_200 IS NOT NULL
        ) subquery
        WHERE pattern IS NOT NULL  -- ✅ Prevent NULL patterns
        ON CONFLICT (symbol, trade_date, pattern) DO NOTHING;

        -- Detect High Volatility (>5% intra-day movement)

        INSERT INTO trading_patterns (symbol, trade_date, pattern_category, pattern, confidence_score)
        SELECT symbol, trade_date, 'Volatility',
            'High Volatility (>5%)' AS pattern,
            ((highest_price - lowest_price) / NULLIF(lowest_price, 0)) * 100 AS confidence_score
        FROM stock_price_history
        WHERE lowest_price > 0  -- ✅ Prevents division by zero
        AND ((highest_price - lowest_price) / NULLIF(lowest_price, 0)) * 100 > 5
        ON CONFLICT (symbol, trade_date, pattern) DO NOTHING;

    """

    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    pg_hook.run(sql_query)
    
    log.info("✅ Trading patterns detected and stored.")

# ✅ Slack Notifications
def send_slack_notification(context, status):
    """Send Slack notification using Airflow connection."""
    messages = {
        "success": "✅ *Stock Price History Update Completed Successfully!* 📊",
        "failure": "❌ *Stock Price History Update Failed!* ⚠️",
    }
    
    message = messages.get(status, "ℹ️ *Stock Price History Update Status Unknown!*")
    
    slack_alert = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id="slack_connection",
        message=message,
        username="airflow_bot",
    )
    return slack_alert.execute(context=context)

# Success Callback
def slack_success_callback(context):
    return send_slack_notification(context, "success")

# Failure Callback
def slack_failure_callback(context):
    return send_slack_notification(context, "failure")


# ✅ Define DAG
with DAG(
    dag_id="full_stock_pipeline",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2025, 1, 4),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="00 1 * * 1-5",
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
        python_callable=update_stock_price_history,
    )

    update_stock_task = PythonOperator(
        task_id="update_stock_price_history",
        python_callable=update_stock_price_history,
    )

    detect_patterns_task = PythonOperator(
        task_id="detect_trading_patterns",
        python_callable=detect_trading_patterns,
    )

    send_slack_notification_task = PythonOperator(
    task_id="slack_notification",
    python_callable=send_slack_notification,
    provide_context=True,  # ✅ Ensures Airflow passes execution context
    op_args=[None],  # ✅ Placeholder for 'context' (Airflow auto-populates)
    op_kwargs={"status": "success"},  # ✅ Explicitly passing 'status'
)




    # ✅ Define DAG sequence
    fetch_stock_task >> upload_json_task >> process_parquet_task >> load_postgres_task >> update_stock_task >> detect_patterns_task >> send_slack_notification_task
