import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

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



# Airflow DAG setup
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "detect_trading_patterns",
    default_args=default_args,
    description="Detect and store stock trading patterns weekly",
    schedule_interval="30 2 * * 1",  # ✅ Runs at 02:30 UTC every Monday
    catchup=False,
    tags=["trading-patterns", "postgres", "weekly-update"],
) as dag:


    detect_patterns_task = PythonOperator(
        task_id="detect_trading_patterns",
        python_callable=detect_trading_patterns,
    )

    detect_patterns_task
