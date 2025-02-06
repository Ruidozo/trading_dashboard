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
    
def load_company_news(symbol):
    """Fetches the latest news for a given company symbol."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = f"""
    SELECT news_date, headline, source, url
    FROM daily_company_news
    WHERE symbol = '{symbol}'
    ORDER BY news_date DESC
    LIMIT 5
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except SQLAlchemyError as e:
        st.error(f"❌ Database query failed: {e}")
        return pd.DataFrame()
    
def load_trading_patterns():
    """Fetches trading patterns from the trading_patterns table."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT tp.symbol AS "Symbol", tc.name AS "Company Name", tp.pattern AS "Pattern", 
           tp.trade_date AS "Date", tp.confidence_score AS "Confidence Score", 
           tp.pattern_category AS "Category"
    FROM trading_patterns tp
    JOIN tech_companies tc ON tp.symbol = tc.symbol
    WHERE tp.trade_date = (SELECT MAX(trade_date) FROM trading_patterns)
    ORDER BY tc.rank ASC
    LIMIT 100
    """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except SQLAlchemyError as e:
        st.error(f"❌ Database query failed: {e}")
        return pd.DataFrame()

def load_top_gainers():
    """Fetch top market gainers."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT company_name AS "Name", t.trade_date AS "Date", t.symbol, t.closing_price AS "Price", 
           t.previous_closing_price AS "Previous Price",
           ROUND((t.closing_price - t.previous_closing_price), 2) AS "Price Change", 
           ROUND(((t.closing_price - t.previous_closing_price) / t.previous_closing_price) * 100, 2) AS "Percent Change"
    FROM stock_price_history t
    JOIN (
        SELECT symbol, MAX(trade_date) AS latest_trade_date
        FROM stock_price_history
        GROUP BY symbol
    ) sub ON t.symbol = sub.symbol AND t.trade_date = sub.latest_trade_date
    WHERE t.previous_closing_price != 0
    ORDER BY "Percent Change" DESC LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_top_losers():
    """Fetch top market losers."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT company_name AS "Name", t.trade_date AS "Date", t.symbol, t.closing_price AS "Price", 
           t.previous_closing_price AS "Previous Price",
           ROUND((t.closing_price - t.previous_closing_price), 2) AS "Price Change", 
           ROUND(((t.closing_price - t.previous_closing_price) / t.previous_closing_price) * 100, 2) AS "Percent Change"
    FROM stock_price_history t
    JOIN (
        SELECT symbol, MAX(trade_date) AS latest_trade_date
        FROM stock_price_history
        GROUP BY symbol
    ) sub ON t.symbol = sub.symbol AND t.trade_date = sub.latest_trade_date
    WHERE t.previous_closing_price != 0
    ORDER BY "Percent Change" ASC LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_market_trends():
    """Fetch general market trends."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT symbol, closing_price, 
           ROUND((closing_price - previous_closing_price), 2) AS price_change, 
           ROUND(((closing_price - previous_closing_price) / previous_closing_price) * 100, 2) AS percent_change
    FROM stock_price_history
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_price_history)
    AND previous_closing_price != 0
    ORDER BY percent_change DESC LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_market_behavior():
    """Fetch and aggregate market data to show daily behavior."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT trade_date AS "Date", AVG(closing_price) AS "Average Price"
    FROM stock_price_history
    GROUP BY trade_date
    ORDER BY trade_date
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df

def load_high_volatility_stocks():
    """Fetch high volatility stocks."""
    engine = get_db_connection()
    if not engine:
        return pd.DataFrame()

    query = """
    SELECT symbol, trade_date AS "Date", closing_price AS "Price", 
           ROUND((highest_price - lowest_price ), 2) AS "Price Range", 
           ROUND(((highest_price - lowest_price ) / lowest_price ) * 100, 2) AS "Percent Range"
    FROM stock_price_history
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_price_history)
    AND lowest_price  != 0
    ORDER BY "Percent Range" DESC LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df