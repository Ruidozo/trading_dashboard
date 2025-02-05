import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression, LinearRegression
import numpy as np


def execute_sql():
    """Runs the SQL model to analyze stock movements based on news sentiment."""
    sql_query = """
            WITH recent_news AS (
            SELECT 
                symbol, 
                news_date, 
                AVG(sentiment_score) AS avg_sentiment_score,
                COUNT(*) AS news_count
            FROM daily_company_news
            WHERE news_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY symbol, news_date
        ),
        price_changes AS (
            SELECT 
                symbol,
                trade_date,
                COALESCE(
                    (closing_price - LAG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date)) / 
                    NULLIF(LAG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date), 0) * 100, 
                    0
                ) AS price_change,
                COALESCE((highest_price - lowest_price) / NULLIF(lowest_price, 0) * 100, 0) AS volatility_score,
                CASE 
                    WHEN closing_price > LAG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date) THEN 'Up'
                    WHEN closing_price < LAG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date) THEN 'Down'
                    ELSE 'No Change'
                END AS price_direction
            FROM stock_price_history
            WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'
            AND closing_price IS NOT NULL
        ),
        merged_data AS (
            SELECT 
                n.symbol,
                n.news_date,
                n.avg_sentiment_score,
                p.price_change,
                p.price_direction,
                p.volatility_score,
                n.news_count
            FROM recent_news n
            LEFT JOIN price_changes p ON n.symbol = p.symbol AND n.news_date = p.trade_date
        )
        INSERT INTO news_stock_analysis 
        SELECT * FROM merged_data
        ON CONFLICT (symbol, news_date) DO UPDATE 
        SET avg_sentiment_score = EXCLUDED.avg_sentiment_score,
            price_change = EXCLUDED.price_change,
            price_direction = EXCLUDED.price_direction,
            volatility_score = EXCLUDED.volatility_score,
            news_count = EXCLUDED.news_count;

    """
    
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    pg_hook.run(sql_query)
    logging.info("✅ News & stock pattern analysis completed successfully.")

def train_ml_model():
    """Trains an ML model using stock history data and ensures no NaN values exist."""
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")

    sql_query = """
        SELECT symbol, trade_date, opening_price, highest_price, lowest_price, closing_price, traded_volume
        FROM stock_price_history
        WHERE trade_date >= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY symbol, trade_date;
    """

    df = pd.read_sql(sql_query, pg_hook.get_conn())

    logging.info(f"📊 Loaded dataset: {df.shape[0]} rows")

    if df.empty:
        logging.warning("⚠️ Not enough stock data to train the model. Skipping training.")
        return

    # ✅ Compute additional features
    df['price_change'] = df.groupby('symbol')['closing_price'].pct_change().fillna(0) * 100  # % change
    df['volatility'] = ((df['highest_price'] - df['lowest_price']) / df['lowest_price']).fillna(0) * 100  # Volatility %

    # ✅ Ensure no NaN or Inf values
    logging.info(f"🔍 Checking NaN values BEFORE filling:\n{df.isnull().sum()}")
    df.fillna(0, inplace=True)
    df.replace([np.inf, -np.inf], 0, inplace=True)
    logging.info(f"🔍 Checking NaN values AFTER filling:\n{df.isnull().sum()}")

    # ✅ Define Features & Target
    X = df[['opening_price', 'highest_price', 'lowest_price', 'closing_price', 'traded_volume', 'price_change', 'volatility']]

    # ✅ Drop last row per stock before defining `y`
    df = df.sort_values(["symbol", "trade_date"])  # Ensure order
    df = df.groupby("symbol").apply(lambda x: x.iloc[:-1]).reset_index(drop=True)  # Drop last row per stock

    # Recalculate `y` after dropping rows
    y = df.groupby("symbol")['closing_price'].shift(-1)  # Predict next day's closing price

    # ✅ Drop rows where `y` is NaN after shifting
    y = y.dropna()
    df = df.loc[y.index]

    # ✅ Ensure X and y have the same length
    X = X.iloc[:len(y)]  # Trim `X` to match the valid `y` values

    # 🔍 Check if `y` is now clean
    logging.info(f"🔍 Checking NaN values in target (`y`) after fix: {y.isnull().sum()}")
    logging.info(f"🔍 Shape of X: {X.shape}, Shape of y: {y.shape}")

    # ✅ Verify if any NaN or Inf in X and y
    assert not np.any(np.isnan(X)), "X contains NaN values"
    assert not np.any(np.isinf(X)), "X contains infinite values"
    assert not np.any(np.isnan(y)), "y contains NaN values"
    assert not np.any(np.isinf(y)), "y contains infinite values"

    # Train the model
    model = LinearRegression()
    model.fit(X, y)

    # ✅ Save model
    model_path = "/opt/airflow/models"
    os.makedirs(model_path, exist_ok=True)
    joblib.dump(model, os.path.join(model_path, 'stock_prediction_model.pkl'))

    logging.info("✅ Stock price model trained and saved successfully.")

def predict_stock_movement():
    """Predicts stock movements using the latest available stock data, ensuring previous_closing_price is updated."""
    
    model_path = '/opt/airflow/models/stock_prediction_model.pkl'
    if not os.path.exists(model_path):
        logging.error(f"❌ Model file not found: {model_path}")
        return

    model = joblib.load(model_path)

    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # ✅ Step 1: Fetch latest stock data & compute previous closing price
    sql_query = """
    WITH latest_stock_data AS (
        SELECT s.symbol, 
            s.trade_date, 
            t.name AS company_name,
            t.rank AS market_cap_rank,
            s.opening_price, 
            s.highest_price, 
            s.lowest_price, 
            s.closing_price, 
            s.traded_volume,
            -- Compute price change and volatility
            (s.closing_price - LAG(s.closing_price) OVER (PARTITION BY s.symbol ORDER BY s.trade_date)) 
            / NULLIF(LAG(s.closing_price) OVER (PARTITION BY s.symbol ORDER BY s.trade_date), 0) * 100 
            AS price_change,
            (s.highest_price - s.lowest_price) / NULLIF(s.lowest_price, 0) * 100 AS volatility,
            -- Fetch the latest known closing price from stock_price_history
            (SELECT closing_price 
             FROM stock_price_history sph
             WHERE sph.symbol = s.symbol
             AND sph.trade_date < s.trade_date
             ORDER BY sph.trade_date DESC
             LIMIT 1) AS previous_closing_price
        FROM stock_price_history s
        LEFT JOIN tech_companies t ON s.symbol = t.symbol
        WHERE s.trade_date = (SELECT MAX(trade_date) FROM stock_price_history)
    )
    SELECT * FROM latest_stock_data WHERE previous_closing_price IS NOT NULL;
    """

    df = pd.read_sql(sql_query, conn)

    if df.empty:
        logging.warning("⚠️ No stock data available for prediction.")
        cursor.close()
        return

    df.fillna(0, inplace=True)

    # ✅ Step 2: Prepare data for prediction (Exclude previous_closing_price)
    features_for_prediction = ['opening_price', 'highest_price', 'lowest_price', 'closing_price', 
                               'traded_volume', 'price_change', 'volatility']  # 7 features only

    X = df[features_for_prediction]

    # ✅ Step 3: Make predictions
    predictions = model.predict(X)
    df["predicted_closing_price"] = predictions
    df["trade_date"] += pd.Timedelta(days=1)  # Predict for the next trading day

    # ✅ Step 4: Insert new predictions into `stock_predictions`
    selected_columns = ['trade_date', 'symbol', 'company_name', 'market_cap_rank', 'previous_closing_price', 'predicted_closing_price']
    df = df[selected_columns]

    insert_query = """
    INSERT INTO stock_predictions (trade_date, symbol, company_name, market_cap_rank, previous_closing_price, predicted_closing_price)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (trade_date, symbol) 
    DO UPDATE SET 
        company_name = EXCLUDED.company_name,
        market_cap_rank = EXCLUDED.market_cap_rank,
        previous_closing_price = EXCLUDED.previous_closing_price,
        predicted_closing_price = EXCLUDED.predicted_closing_price;
    """

    for row in df.itertuples(index=False):
        cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()

    logging.info("✅ Stock predictions saved successfully with updated previous_closing_price.")



# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 3),
    "retries": 1,
}

with DAG(
    dag_id="analyze_news_stock_patterns",
    default_args=default_args,
    schedule_interval="0 4 * * 0",  # ✅ Runs every Sunday at 4am
    catchup=False,
    tags=["news", "stocks", "analysis", "ml"],
) as dag:
    run_analysis = PythonOperator(
        task_id="run_news_stock_analysis",
        python_callable=execute_sql,
    )
    
    train_model = PythonOperator(
        task_id="train_ml_model",
        python_callable=train_ml_model,
    )
    
    predict_movement = PythonOperator(
        task_id="predict_stock_movement",
        python_callable=predict_stock_movement,
    )

    run_analysis >> train_model >> predict_movement
