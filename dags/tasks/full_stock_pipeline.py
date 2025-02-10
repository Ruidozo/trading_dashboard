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

# ✅ Environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_BUCKET_PROCESSED = os.getenv("GCS_BUCKET_PROCESSED")
POSTGRES_CONN_ID = "project_postgres"


if not all([GCS_BUCKET_NAME, GCS_BUCKET_PROCESSED]):
    raise ValueError("🚨 One or more required environment variables are missing.")

# Configure logging
log = logging.getLogger(__name__)

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
    """Fetches stock data from Yahoo Finance and saves it as JSON."""
    logging.info("🔄 Starting stock data fetch...")
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tech_companies")
    companies = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    from datetime import datetime, timedelta

    # Use same helper logic to set last trading day.
    def get_last_trading_day(ref_date):
        # If ref_date is Saturday (5) or Sunday (6), adjust to Friday.
        if ref_date.weekday() == 5:
            return ref_date - timedelta(days=1)
        elif ref_date.weekday() == 6:
            return ref_date - timedelta(days=2)
        return ref_date

    potential_day = (datetime.utcnow() - timedelta(days=1)).date()
    last_trading_day = get_last_trading_day(potential_day)
    date_folder = last_trading_day.strftime('%Y/%m/%d')
    utc_date_str = last_trading_day.strftime('%Y%m%d')
    utc_date_formatted = last_trading_day.strftime('%Y-%m-%d')

    for symbol in companies:
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1d", interval="1d")  # Last available day's data

            if hist.empty:
                logging.warning(f"⚠️ No valid data received for {symbol}. Skipping.")
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
            # Overwrite trade_date to match the computed last_trading_day
            data = [{**row, "trade_date": utc_date_formatted} for row in data]

            full_path = os.path.join("/opt/airflow/data/raw/", date_folder)
            os.makedirs(full_path, exist_ok=True)

            file_path = os.path.join(full_path, f"{symbol}_{utc_date_str}.json")
            with open(file_path, "w") as f:
                json.dump(data, f)

            logging.info(f"✅ Data saved: {file_path}")
            time.sleep(0)

        except Exception as e:
            logging.error(f"❌ Error fetching stock data for {symbol}: {e}")
    
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
def process_json_to_parquet(last_trading_day=None):
    """
    Process JSON files and convert them to Parquet.
    :param last_trading_day: (str) Target date in 'YYYY-MM-DD' format. If not provided, defaults to last trading day.
    """
    from datetime import datetime, timedelta

    # Helper to adjust for weekends
    def get_last_trading_day(ref_date):
        # If ref_date is Saturday (5) or Sunday (6), adjust to Friday.
        if ref_date.weekday() == 5:
            return ref_date - timedelta(days=1)
        elif ref_date.weekday() == 6:
            return ref_date - timedelta(days=2)
        return ref_date

    # Determine the target trading day:
    if last_trading_day is None:
        # When no parameter is provided, default to yesterday then adjust for weekend.
        potential_day = (datetime.utcnow() - timedelta(days=1)).date()
        last_trading_day_dt = datetime.combine(get_last_trading_day(potential_day), datetime.min.time())
    elif isinstance(last_trading_day, str):
        dt = datetime.strptime(last_trading_day, "%Y-%m-%d")
        last_trading_day_dt = datetime.combine(get_last_trading_day(dt.date()), datetime.min.time())
    else:
        # Assume a datetime is passed
        last_trading_day_dt = datetime.combine(get_last_trading_day(last_trading_day.date()), datetime.min.time())

    # Build folder prefix using the target trading day's date
    folder_prefix = last_trading_day_dt.strftime('%Y/%m/%d')
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    files = gcs_hook.list(bucket_name=GCS_BUCKET_NAME, prefix=f"{folder_prefix}/")

    if not files:
        logging.warning(f"⚠️ No JSON files found in GCS for {folder_prefix}")
        return

    all_data = []
    for file in files:
        try:
            json_data = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=file)
            json_content = json.loads(json_data.decode('utf-8'))  # Load JSON
            
            # Log the trade dates found in the file
            trade_dates = [entry.get("trade_date") for entry in json_content if isinstance(entry, dict)]
            logging.info(f"DEBUG: File '{file}' contains trade_date values: {trade_dates}")

            if not isinstance(json_content, list):
                logging.error(f"❌ Unexpected JSON structure in {file}")
                continue

            symbol = file.split("/")[-1].split("_")[0]
            target_date = last_trading_day_dt.strftime('%Y-%m-%d')

            # Extract only the record for the target date
            filtered_data = [entry for entry in json_content if entry.get("trade_date") == target_date]

            if not filtered_data:
                logging.warning(f"⚠️ No matching stock data found for {target_date} in {file}")
                continue

            for entry in filtered_data:
                entry["symbol"] = symbol
                entry["date"] = target_date

            all_data.extend(filtered_data)

        except Exception as e:
            logging.error(f"❌ Error processing {file}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        year_month = last_trading_day_dt.strftime('%Y/%m')
        parquet_filename = f"daily_stocks_{last_trading_day_dt.strftime('%Y-%m-%d')}.parquet"
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
    from datetime import datetime, timedelta

    # Helper to adjust for weekends
    def get_last_trading_day(ref_date):
        if ref_date.weekday() == 5:
            return ref_date - timedelta(days=1)
        elif ref_date.weekday() == 6:
            return ref_date - timedelta(days=2)
        return ref_date

    potential_day = (datetime.utcnow() - timedelta(days=1)).date()
    last_trading_day = get_last_trading_day(potential_day)
    
    year_month = last_trading_day.strftime('%Y/%m')
    parquet_filename = f"daily_stocks_{last_trading_day.strftime('%Y-%m-%d')}.parquet"
    gcs_parquet_path = f"{year_month}/{parquet_filename}"
    local_parquet_path = f"/tmp/{parquet_filename}"
    
    # Instantiate gcs_hook before using it
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    try:
        # ✅ Step 1: Download Parquet from GCS
        logging.info(f"📥 Downloading Parquet from GCS: {gcs_parquet_path}")
        gcs_hook.download(bucket_name=GCS_BUCKET_PROCESSED, object_name=gcs_parquet_path, filename=local_parquet_path)

        # ✅ Step 2: Load Parquet File into DataFrame
        logging.info(f"📂 Attempting to load Parquet file: {local_parquet_path}")
        df = pd.read_parquet(local_parquet_path, engine="pyarrow")

        # ✅ Step 3: Check if DataFrame is Empty
        if df.empty:
            logging.error("❌ DataFrame is EMPTY after reading Parquet file! Aborting insert.")
            return

        logging.info(f"📊 Loaded Parquet DataFrame with {df.shape[0]} rows")
        logging.info(df.head())

        # ✅ Step 4: Drop duplicate 'date' column if it exists
        if "date" in df.columns:
            df = df.loc[:, ~df.columns.duplicated()].copy()

        # ✅ Step 5: Rename 'trade_date' to 'date' for consistency
        df.rename(columns={"trade_date": "date"}, inplace=True)

        # ✅ Step 6: Rename other columns to match PostgreSQL schema
        rename_mapping = {
            "opening_price": "o",
            "highest_price": "h",
            "lowest_price": "l",
            "closing_price": "c",
            "traded_volume": "v",
            "Dividends": "d",
            "Stock Splits": "dp"
        }
        df.rename(columns=rename_mapping, inplace=True)

        # ✅ Step 7: Drop extra columns
        drop_columns = ["Capital Gains"]
        df.drop(columns=[col for col in drop_columns if col in df.columns], inplace=True)

        # ✅ Step 8: Ensure required columns exist
        required_columns = ["symbol", "date", "c", "d", "dp", "h", "l", "o", "v"]
        for col in required_columns:
            if col not in df.columns:
                logging.warning(f"⚠️ Missing column: {col}. Filling with default values.")
                df[col] = 0 if col in ["c", "d", "dp", "h", "l", "o", "v"] else None

        # ✅ Step 9: Convert data types
        df = df.astype({
            "c": float, "h": float, "l": float, "o": float, "v": int,
            "d": float, "dp": float
        })
        if "date" in df.columns:
            df = df.loc[:, ~df.columns.duplicated()].copy()
            df["date"] = pd.to_datetime(df["date"]).dt.date

        # ✅ Ensure data is sorted by symbol and date
        df = df.sort_values(["symbol", "date"])

        # ✅ Assign previous day's closing price to 'pc'
        df["pc"] = df.groupby("symbol")["c"].shift(1)  # Get previous day's closing price

        # ✅ Fill NaN values (e.g., first entry) with 0 or another placeholder
        df["pc"].fillna(0, inplace=True)

        print(df[["symbol", "date", "c", "pc"]].head(10))

        # drop unix_timestamp column if it exists
        if "unix_timestamp" in df.columns:
            df.drop(columns="unix_timestamp", inplace=True)



        logging.info(f"📊 DataFrame ready for PostgreSQL insert:\n{df.dtypes}")
        logging.info(df.head())

        # ✅ Step 10: Connect to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            # 🔍 Step 11: Drop and recreate `staging_stock_data`
            logging.info("🗑️ Dropping `staging_stock_data` table if exists.")
            conn.execute("DROP TABLE IF EXISTS staging_stock_data;")

            # ✅ Step 12: Insert data
            logging.info(f"📤 Inserting {len(df)} rows into `staging_stock_data`.")
            df.to_sql("staging_stock_data", conn, if_exists="replace", index=False)

            # 🔍 Step 13: Verify the inserted data
            result = conn.execute("SELECT COUNT(*) FROM staging_stock_data;")
            count = result.fetchone()[0]
            logging.info(f"🔍 Rows in `staging_stock_data` after insert: {count}")

            if count == 0:
                logging.error("❌ No rows inserted into `staging_stock_data`! Check logs.")

        logging.info("✅ Staging table updated successfully!")

    except Exception as e:
        logging.error(f"❌ Error in `load_parquet_to_postgres`: {e}")
        raise



# ✅ Update Stock Price History
def update_stock_price_history():
    """Merge `staging_stock_data` into partitioned `stock_price_history`."""
    log.info("🔄 Updating partitioned stock_price_history with latest stock data...")

    sql_query = """
            INSERT INTO stock_price_history (
                trade_date, market_cap_rank, company_name, country, symbol, 
                opening_price, highest_price, lowest_price, closing_price, 
                previous_closing_price, traded_volume
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
                
                -- ✅ Use LAG() first, then fallback to last known closing_price
                COALESCE(
                    LAG(s.c) OVER (PARTITION BY s.symbol ORDER BY s.date),
                    (SELECT closing_price FROM stock_price_history h
                    WHERE h.symbol = s.symbol AND h.trade_date < s.date
                    ORDER BY h.trade_date DESC LIMIT 1)
                ) AS previous_closing_price,
                
                COALESCE(s.v, 0) AS traded_volume
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
                traded_volume = EXCLUDED.traded_volume;

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
