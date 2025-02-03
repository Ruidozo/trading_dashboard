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
    SELECT DISTINCT company_name, symbol, market_cap_rank 
    FROM stock_price_history 
    ORDER BY market_cap_rank ASC
    """
    df = pd.read_sql(query, engine)
    engine.dispose()

    # Ensure market cap rank is included
    df["dropdown_label"] = df["company_name"] + " (" + df["symbol"] + ")"
    
    return df.to_dict(orient="records")


def load_stock_data(company_name=None, start_date=None, end_date=None, limit=100):
    """Fetch stock data with optional filters."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = "SELECT * FROM stock_price_history"
    filters = []
    
    if company_name:
        filters.append(f"company_name = '{company_name}'")
    if start_date and end_date:
        filters.append(f"trade_date BETWEEN '{start_date}' AND '{end_date}'")
    
    if filters:
        query += " WHERE " + " AND ".join(filters)
    
    query += f" ORDER BY trade_date DESC LIMIT {limit}"

    print(f"🔍 Running Query: {query}")  # ✅ Print the query for debugging
    df = pd.read_sql(query, engine)
    engine.dispose()

    print(f"📊 Query Returned {len(df)} rows")  # ✅ Show how many rows are returned
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

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)  # ✅ Use `conn`, not `engine`
        return df
    except Exception as e:
        print(f"Database query error: {e}")
        return pd.DataFrame()


