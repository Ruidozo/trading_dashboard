import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import joblib
from sklearn.linear_model import LogisticRegression

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
    """Trains an ML model to predict stock movement based on news sentiment."""
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    sql_query = "SELECT avg_sentiment_score, news_count, price_change, price_direction, volatility_score FROM news_stock_analysis"
    df = pd.read_sql(sql_query, pg_hook.get_conn())
    
    logging.info(f"📊 Loaded dataset: {df.shape[0]} rows")
    logging.info(f"🔍 Checking missing values:\n{df.isnull().sum()}")

    if df.empty:
        logging.warning("⚠️ Not enough data to train the model. Skipping training.")
        return
    
    df.fillna(0, inplace=True)  # Replace NaN values with 0 to prevent errors
    
    df['price_direction'] = df['price_direction'].map({'Up': 1, 'Down': 0})
    X = df[['avg_sentiment_score', 'news_count', 'volatility_score']]
    y = df['price_direction']
    
    if X.empty or y.empty or (X == 0).all().all():
        logging.warning("⚠️ Skipping training: No valid or non-zero data points available.")
        return
    
    model = LogisticRegression()
    model.fit(X, y)
    
    joblib.dump(model, '/opt/airflow/models/stock_prediction_model.pkl')
    logging.info("✅ ML Model trained and saved successfully.")

def predict_stock_movement():
    """Uses trained model to predict stock movements for the next week."""
    model = joblib.load('/opt/airflow/models/stock_prediction_model.pkl')
    pg_hook = PostgresHook(postgres_conn_id="project_postgres")
    sql_query = "SELECT avg_sentiment_score, news_count, volatility_score, symbol FROM news_stock_analysis WHERE news_date >= CURRENT_DATE - INTERVAL '1 day'"
    df = pd.read_sql(sql_query, pg_hook.get_conn())
    
    if df.empty:
        logging.warning("⚠️ Not enough data to make predictions. Skipping prediction task.")
        return
    
    df.fillna(0, inplace=True)  # Replace NaN values with 0 to handle missing values
    
    X = df[['avg_sentiment_score', 'news_count', 'volatility_score']]
    predictions = model.predict(X)
    df['predicted_direction'] = ['Up' if p == 1 else 'Down' for p in predictions]
    
    df.to_sql("predicted_stock_movements", pg_hook.get_conn(), if_exists='replace', index=False)
    logging.info("✅ Predictions stored successfully.")

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
    schedule_interval="0 0 * * 0",  # ✅ Runs every Sunday at midnight
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
