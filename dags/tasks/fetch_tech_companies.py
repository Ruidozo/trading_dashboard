import os
import logging
import pandas as pd
import requests
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Logger setup
log = logging.getLogger(__name__)

# CSV URL
CSV_URL = "https://companiesmarketcap.com/tech/largest-tech-companies-by-market-cap/?download=csv"
CSV_FILE_PATH = "/opt/airflow/data/tech_companies.csv"

def download_tech_companies_csv():
    """Download the latest top tech companies by market cap and save locally."""
    log.info("📥 Downloading latest tech companies CSV...")
    response = requests.get(CSV_URL)
    
    if response.status_code == 200:
        with open(CSV_FILE_PATH, "wb") as file:
            file.write(response.content)
        log.info("✅ CSV Downloaded successfully!")
    else:
        log.error(f"❌ Failed to download CSV. Status code: {response.status_code}")
        raise Exception("Failed to download CSV")

def process_tech_companies():
    """Creates a new PostgreSQL table exactly matching the CSV structure and filters out unsupported symbols."""
    log.info("📊 Processing tech companies CSV...")

    # Read CSV
    df = pd.read_csv(CSV_FILE_PATH)

    # Ensure correct column names
    expected_columns = ['Rank', 'Name', 'Symbol', 'marketcap', 'price (USD)', 'country']
    if not all(col in df.columns for col in expected_columns):
        log.error(f"❌ Unexpected CSV format! Columns found: {df.columns}")
        raise Exception("CSV format mismatch")

    # Rename columns for consistency
    df.columns = ['rank', 'name', 'symbol', 'market_cap', 'price_usd', 'country']

    # Drop rows where `symbol` is missing
    df = df.dropna(subset=['symbol'])

    # Convert `symbol` to string to avoid issues with NaN values
    df['symbol'] = df['symbol'].astype(str)

    # Convert market capitalization to numeric
    df['market_cap'] = df['market_cap'].replace('[\$,]', '', regex=True).astype(float)
    df['price_usd'] = df['price_usd'].replace('[\$,]', '', regex=True).astype(float)

    # ✅ Filter: Keep only symbols that Finnhub supports (only uppercase letters, no numbers, no ".KS" etc.)
    df = df[df['symbol'].str.match(r'^[A-Z]+$', na=False)]  # Ensures no NaN values

    log.info(f"✅ Filtered tech companies: {len(df)} remain after removing unsupported symbols.")

    # Store in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Drop old table and create a new one
    create_table_query = """
    DROP TABLE IF EXISTS tech_companies;
    CREATE TABLE tech_companies (
        rank INT PRIMARY KEY,
        name TEXT,
        symbol TEXT UNIQUE,
        market_cap NUMERIC,
        price_usd NUMERIC,
        country TEXT
    );
    """
    cursor.execute(create_table_query)

    # Insert data into PostgreSQL
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO tech_companies (rank, name, symbol, market_cap, price_usd, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
            name = EXCLUDED.name,
            market_cap = EXCLUDED.market_cap,
            price_usd = EXCLUDED.price_usd,
            country = EXCLUDED.country;
        """, (row['rank'], row['name'], row['symbol'], row['market_cap'], row['price_usd'], row['country']))

    conn.commit()
    cursor.close()
    conn.close()

    log.info("✅ Stored filtered tech companies in PostgreSQL!")

# ✅ Convert to TaskGroup Function
def fetch_tech_companies_taskgroup(dag):
    with TaskGroup("fetch_tech_companies", dag=dag) as fetch_tech_companies:
        download_task = PythonOperator(task_id="download_csv", python_callable=download_tech_companies_csv)
        process_task = PythonOperator(task_id="process_tech_companies", python_callable=process_tech_companies)
        download_task >> process_task
    return fetch_tech_companies
