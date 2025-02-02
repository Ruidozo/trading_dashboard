import streamlit as st
import pandas as pd
from datetime import date
import plotly.express as px
from utils.data_loader import (
    load_company_list,
    load_general_market_data,
    load_stock_data,
    load_high_volatility_patterns,
    load_moving_average_crosses,
    load_recent_trading_patterns,
    load_trend_patterns
)
from components.charts import plot_dark_candlestick_chart
from components.indicators import calculate_sma, calculate_ema, calculate_rsi, calculate_bollinger_bands

st.set_page_config(page_title="Tech Stock Market Dashboard", layout="wide")

# Sidebar Navigation
st.sidebar.header("Navigation")
selected_page = st.sidebar.radio("Choose a View:", ["Company Insights", "General Market Trends", "Stock Comparison"])

if selected_page == "Company Insights":
    # COMPANY-SPECIFIC INSIGHTS SECTION
    st.sidebar.header("Select a Company")
    company_options = load_company_list()

    if company_options:
        company_labels = [company["dropdown_label"] for company in company_options]
        selected_label = st.sidebar.selectbox("Select a Company", company_labels)
        selected_company = next((c for c in company_options if c["dropdown_label"] == selected_label), None)

        st.sidebar.header("Select Date Range")
        start_date = st.sidebar.date_input("Start Date", date.today().replace(day=1))
        end_date = st.sidebar.date_input("End Date", date.today())

        if start_date > end_date:
            st.sidebar.error("Warning: Start date must be before end date.")

        if selected_company:
            selected_company_name = selected_company["company_name"]
            selected_symbol = selected_company["symbol"]
            selected_market_cap_rank = selected_company.get("market_cap_rank", "N/A")

            st.title(f"{selected_company_name} ({selected_symbol}) Market Overview")

            df = load_stock_data(company_name=selected_company_name, start_date=start_date, end_date=end_date)

            if not df.empty:
                st.metric(label="Market Cap Rank", value=f"{selected_market_cap_rank}")
                df = calculate_sma(df, window=14)
                df = calculate_ema(df, window=14)
                df = calculate_rsi(df, window=14)
                df = calculate_bollinger_bands(df, window=20)
                
                st.write("### Stock Price History")
                st.dataframe(df)

                # Candlestick Chart
                st.subheader("Stock Price Movement")
                candlestick_fig = plot_dark_candlestick_chart(df, selected_company_name)
                st.plotly_chart(candlestick_fig, use_container_width=True)
                
                # Technical Indicators
                st.subheader("Technical Indicators")
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Simple Moving Average (SMA)**")
                    st.line_chart(df.set_index("trade_date")[["closing_price", "SMA_14"]])

                    st.write("**Relative Strength Index (RSI)**")
                    st.line_chart(df.set_index("trade_date")[["RSI"]])

                with col2:
                    st.write("**Exponential Moving Average (EMA)**")
                    st.line_chart(df.set_index("trade_date")[["closing_price", "EMA_14"]])

                    st.write("**Bollinger Bands**")
                    st.line_chart(df.set_index("trade_date")[["closing_price", "Upper Band", "Lower Band"]])
            else:
                st.warning("No data available for the selected company.")

elif selected_page == "Stock Comparison":
    # MULTI-STOCK COMPARISON SECTION
    st.title("Compare Two Stocks")
    
    company_options = load_company_list()
    
    if company_options:
        company_labels = [company["dropdown_label"] for company in company_options]
        col1, col2 = st.columns(2)
        
        with col1:
            selected_label_1 = st.selectbox("Select First Company", company_labels, key="comp1")
            selected_company_1 = next((c for c in company_options if c["dropdown_label"] == selected_label_1), None)
        
        with col2:
            selected_label_2 = st.selectbox("Select Second Company", company_labels, key="comp2")
            selected_company_2 = next((c for c in company_options if c["dropdown_label"] == selected_label_2), None)
        
        if selected_company_1 and selected_company_2:
            selected_symbol_1 = selected_company_1["symbol"]
            selected_symbol_2 = selected_company_2["symbol"]
            
            df_1 = load_stock_data(company_name=selected_company_1["company_name"])
            df_2 = load_stock_data(company_name=selected_company_2["company_name"])
            
            if not df_1.empty and not df_2.empty:
                st.subheader("Stock Price Comparison")
                
                fig = px.line()
                fig.add_scatter(x=df_1["trade_date"], y=df_1["closing_price"], mode='lines', name=selected_company_1["company_name"])
                fig.add_scatter(x=df_2["trade_date"], y=df_2["closing_price"], mode='lines', name=selected_company_2["company_name"])
                fig.update_layout(title_text="Stock Price Over Time", xaxis_title="Date", yaxis_title="Closing Price")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No data available for one or both selected companies.")


elif selected_page == "General Market Trends":
    # 🌍 GENERAL MARKET TRENDS SECTION
    st.title("General Market Overview")

    # 📊 Top Market Gainers & Losers
    st.subheader("Top Market Gainers & Losers")
    query_gainers_losers = """
    SELECT symbol, company_name, closing_price, 
           ROUND((closing_price - previous_closing_price), 2) AS price_change, 
           ROUND(((closing_price - previous_closing_price) / previous_closing_price) * 100, 2) AS percent_change
    FROM stock_price_history
    ORDER BY percent_change DESC LIMIT 10;
    """
    df_gainers_losers = load_general_market_data(query_gainers_losers)
    
    if not df_gainers_losers.empty:
        st.dataframe(df_gainers_losers)
    else:
        st.warning("No market data available.")

    # 📈 High Volatility Stocks
    st.subheader("High Volatility Patterns")
    df_market_volatility = load_high_volatility_patterns()
    if not df_market_volatility.empty:
        st.dataframe(df_market_volatility)
    else:
        st.warning("No high volatility stocks found.")

    # 📊 Moving Average Crosses
    st.subheader("Moving Average Crosses")
    df_moving_average_crosses = load_moving_average_crosses()
    if not df_moving_average_crosses.empty:
        st.dataframe(df_moving_average_crosses)
    else:
        st.warning("No moving average crosses detected.")

    # 🔄 Recent Trading Patterns
    st.subheader("Recent Trading Patterns")
    df_trading_patterns = load_recent_trading_patterns()
    if not df_trading_patterns.empty:
        st.dataframe(df_trading_patterns)
    else:
        st.warning("No recent trading patterns found.")

    # 📊 Market Trend Analysis
    st.subheader("Market Trend Patterns")
    df_trend_patterns = load_trend_patterns()
    if not df_trend_patterns.empty:
        st.dataframe(df_trend_patterns)
    else:
        st.warning("No trend patterns detected.")

