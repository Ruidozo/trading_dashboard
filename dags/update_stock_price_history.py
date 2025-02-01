import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# Configure logging
log = logging.getLogger(__name__)

def update_stock_price_history():
    """Merge `staging_stock_data` into partitioned `stock_price_history` while enforcing integrity."""
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

# Define DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),  # ✅ REQUIRED: Set a valid start date
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG   
with DAG(
    dag_id="update_stock_price_history",
    default_args=default_args,
    description="Merge staging_stock_data into partitioned stock_price_history daily",
    schedule_interval="0 2 * * 1-5",  # ✅ Runs daily at 01:00 UTC
    catchup=False,
    tags=["stock-data", "postgres", "daily-update"],
) as dag:


    update_task = PythonOperator(
        task_id="update_stock_price_history",
        python_callable=update_stock_price_history,
    )

    update_task
