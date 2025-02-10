import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import joblib
from airflow.utils.task_group import TaskGroup
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler
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
    """Trains an ML model using all available stock history data and a correction model that learns prediction errors."""
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")

    sql_query = """
        SELECT symbol, trade_date, opening_price, highest_price, lowest_price, closing_price, traded_volume,
            LAG(closing_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS previous_closing_price
        FROM stock_price_history
        ORDER BY symbol, trade_date;
    """

    df = pd.read_sql(sql_query, pg_hook.get_conn())

    logging.info(f"📊 Loaded dataset: {df.shape[0]} rows")

    if df.empty:
        logging.warning("⚠️ Not enough stock data to train the model. Skipping training.")
        return

    # Compute additional features
    df['price_change'] = df.groupby('symbol')['closing_price'].pct_change() * 100
    df['volatility'] = ((df['highest_price'] - df['lowest_price']) / df['lowest_price']) * 100

    # Fill NaNs
    df['previous_closing_price'].fillna(df['closing_price'], inplace=True)
    df.fillna(0, inplace=True)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna(0, inplace=True)

    # Select Features & Target
    features = ['opening_price', 'highest_price', 'lowest_price', 'closing_price', 
                'traded_volume', 'price_change', 'volatility', 'previous_closing_price']
    df = df.dropna(subset=['closing_price'])
    X = df[features]
    y = df['closing_price'].shift(-1).dropna()
    X = X.iloc[:len(y)]

    # Check for any remaining NaNs or Inf values
    logging.info(f"🔍 NaN values before scaling:\n{X.isnull().sum()}")
    logging.info(f"🔍 Checking for Inf values: {np.isinf(X).sum().sum()}")

    if X.isnull().sum().sum() > 0 or np.isinf(X).sum().sum() > 0:
        logging.error("❌ Data still contains NaN or Inf values. Skipping training.")
        return

    # Normalize the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train primary model
    primary_model = LinearRegression()
    primary_model.fit(X_scaled, y)

    # Calculate error margin for the primary model
    primary_preds = primary_model.predict(X_scaled)
    mae = mean_absolute_error(y, primary_preds)
    rmse = mean_squared_error(y, primary_preds, squared=False)
    logging.info(f"📉 Error margin for primary model: MAE = {mae:.2f}, RMSE = {rmse:.2f}")

    # Compute residuals (error) and train a correction model
    residuals = y - primary_preds
    correction_model = LinearRegression()
    correction_model.fit(X_scaled, residuals)

    # Save both models & scaler
    model_path = "/opt/airflow/models"
    os.makedirs(model_path, exist_ok=True)
    joblib.dump(primary_model, os.path.join(model_path, 'stock_primary_model.pkl'))
    joblib.dump(correction_model, os.path.join(model_path, 'stock_correction_model.pkl'))
    joblib.dump(scaler, os.path.join(model_path, 'stock_scaler.pkl'))

    logging.info("✅ Stock price models (primary & correction) trained and saved successfully.")


def predict_stock_movement():
    """Predicts stock movements using the latest available stock data and adjusts predictions using a correction model."""
    model_path = '/opt/airflow/models'
    primary_model_path = os.path.join(model_path, 'stock_primary_model.pkl')
    correction_model_path = os.path.join(model_path, 'stock_correction_model.pkl')
    scaler_path = os.path.join(model_path, 'stock_scaler.pkl')

    if not (os.path.exists(primary_model_path) and os.path.exists(correction_model_path) and os.path.exists(scaler_path)):
        logging.error("❌ One or more model/scaler files not found.")
        return

    primary_model = joblib.load(primary_model_path)
    correction_model = joblib.load(correction_model_path)
    scaler = joblib.load(scaler_path)

    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

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
            (s.closing_price - LAG(s.closing_price) OVER (PARTITION BY s.symbol ORDER BY s.trade_date)) 
            / NULLIF(LAG(s.closing_price) OVER (PARTITION BY s.symbol ORDER BY s.trade_date), 0) * 100 
            AS price_change,
            (s.highest_price - s.lowest_price) / NULLIF(s.lowest_price, 0) * 100 AS volatility,
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

    features_for_prediction = ['opening_price', 'highest_price', 'lowest_price', 'closing_price', 
                               'traded_volume', 'price_change', 'volatility', 'previous_closing_price']
    X = df[features_for_prediction]
    X_scaled = scaler.transform(X)

    # Get predictions from both models and adjust the final prediction
    primary_preds = primary_model.predict(X_scaled)
    correction_preds = correction_model.predict(X_scaled)
    final_predictions = primary_preds + correction_preds

    df["predicted_closing_price"] = final_predictions
    df["trade_date"] += pd.Timedelta(days=1)

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
    logging.info("✅ Stock predictions saved successfully with adjusted predictions.")


# ✅ Convert to TaskGroup Function
def analyze_news_stock_patterns_taskgroup(dag):
    with TaskGroup("analyze_news_stock_patterns", dag=dag) as analyze_news_stock_patterns:
        run_analysis = PythonOperator(task_id="run_news_stock_analysis", python_callable=execute_sql)
        train_model = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model)
        predict_movement = PythonOperator(task_id="predict_stock_movement", python_callable=predict_stock_movement)
        run_analysis >> train_model >> predict_movement
    return analyze_news_stock_patterns
