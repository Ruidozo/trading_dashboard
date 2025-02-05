import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import streamlit as st


# Database connection string (Docker hostname)
DB_URI = "postgresql://odiurdigital:dashboard@project_postgres:5432/project_db"

def get_db_connection():
    """Creates and returns a database connection."""
    try:
        engine = create_engine(DB_URI)
        return engine
    except SQLAlchemyError as e:
        st.error(f"❌ Database connection failed: {e}")  # ✅ Show error inside Streamlit
        return None


def load_company_list():
    """Fetch a distinct list of companies, ordered by market cap rank."""
    engine = get_db_connection()
    if not engine:
        return []

    query = """
    SELECT DISTINCT name AS company_name, symbol, rank AS market_cap_rank 
    FROM tech_companies 
    ORDER BY market_cap_rank ASC
    """
    df = pd.read_sql(query, engine)
    engine.dispose()

    # Ensure market cap rank is included
    df["dropdown_label"] = df["company_name"] + " (" + df["symbol"] + ")"
    
    return df.to_dict(orient="records")

@st.cache_data(ttl=500)  # Set TTL to 0 to disable caching
def load_stock_data(company_name, start_date, end_date):
    # Assuming you have a database connection or a data source
    query = f"""
    SELECT * FROM stock_price_history
    WHERE company_name = '{company_name}'
    AND trade_date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY trade_date DESC
    """
    # Replace with actual data loading logic
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df


def load_high_volatility_patterns():
    """Fetch high volatility trading patterns."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = "SELECT * FROM view_high_volatility_patterns"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_moving_average_crosses():
    """Fetch moving average crossovers."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = "SELECT * FROM view_moving_average_crosses"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_recent_trading_patterns():
    """Fetch recent trading patterns."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = "SELECT * FROM view_recent_trading_patterns"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_trend_patterns():
    """Fetch general trend patterns."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = "SELECT * FROM view_trend_patterns"
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_general_market_data(query):
    """Fetches general market data based on custom queries."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

def load_stock_predictions(symbol):
    """Fetches the latest stock prediction for a given symbol."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = f"""
    SELECT trade_date, predicted_closing_price
    FROM stock_predictions
    WHERE symbol = '{symbol}'
    ORDER BY trade_date DESC
    LIMIT 1
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Database query error: {e}")
        return pd.DataFrame()