from json import load
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
from utils.data_loader import (
    load_company_list,
    load_general_market_data,
    load_stock_data,
    load_high_volatility_patterns,
    load_moving_average_crosses,
    load_recent_trading_patterns,
    load_trend_patterns,
    load_stock_predictions,  # Import the function for stock predictions
    load_company_news
)
from components.charts import plot_dark_candlestick_chart
from components.indicators import calculate_sma, calculate_ema, calculate_rsi, calculate_bollinger_bands

st.set_page_config(page_title="Tech Stock Market Dashboard", layout="wide")

# Sidebar Navigation
st.sidebar.header("Navigation")
selected_page = st.sidebar.radio("Choose a View:", ["Company Insights", "General Market Trends", "Stock Comparison"])

if selected_page == "Company Insights":
    # Sidebar - Select a Company
    st.sidebar.header("Select a Company")
    company_options = load_company_list()

    if company_options:
        company_labels = [company["dropdown_label"] for company in company_options]
        selected_label = st.sidebar.selectbox("Select a Company", company_labels)
        selected_company = next((c for c in company_options if c["dropdown_label"] == selected_label), None)

        # Sidebar - Select Date Range
        st.sidebar.header("Select Date Range")
        end_date = date.today()
        start_date = end_date - timedelta(days=90)  # Default to last 90 days
        start_date = st.sidebar.date_input("Start Date", start_date)
        end_date = st.sidebar.date_input("End Date", end_date)

        if start_date > end_date:
            st.sidebar.error("Warning: Start date must be before end date.")

        if selected_company:
            selected_company_name = selected_company["company_name"]
            selected_symbol = selected_company["symbol"]
            selected_market_cap_rank = selected_company.get("market_cap_rank", "N/A")

            # 🔹 **Header Section (Company Title & Stock Prediction at the Top)**
            header_col1, header_col2 = st.columns([3, 1])  # Title (wide) and prediction (narrow)

            with header_col1:
                st.title(f"{selected_company_name} ({selected_symbol}) Market Overview")
                st.subheader(f"Market Cap Rank: {selected_market_cap_rank}")

            with header_col2:
                prediction_df = load_stock_predictions(selected_symbol)
                if not prediction_df.empty:
                    prediction_date = prediction_df.iloc[0]['trade_date']
                    predicted_price = prediction_df.iloc[0]['predicted_closing_price']
                    st.markdown(
                        f"""
                        <div style="text-align: center; padding: 10px; border: 2px solid gray; border-radius: 8px;">
                            <h3>Stock Prediction</h3>
                            <p><b>{prediction_date}</b></p>
                            <p><h2>{predicted_price:.3f}</h2></p>
                        </div>
                        """,
                    unsafe_allow_html=True)

            # Load Stock Data
            df = load_stock_data(company_name=selected_company_name, start_date=start_date, end_date=end_date)

            if not df.empty:
                df = df.sort_values(by="trade_date", ascending=False)  # Sort by date in descending order
                df = calculate_sma(df, window=14)
                df = calculate_ema(df, window=14)
                df = calculate_rsi(df, window=14)
                df = calculate_bollinger_bands(df, window=20)

                # 🔹 **Candlestick Chart & News Section (Balanced Layout)**
                col1, col2 = st.columns([2, 1])  # Make chart wider, news narrower

                # Candlestick Chart (Taller)
                with col1:
                    st.subheader("Stock Price Movement")
                    candlestick_fig = plot_dark_candlestick_chart(df, selected_company_name)
                    st.plotly_chart(candlestick_fig, use_container_width=True, height=800)  # Increased height for better visibility

                # News Section (Shorter)
                with col2:
                    st.markdown("<h3 style='text-align: center;'>Latest News</h3>", unsafe_allow_html=True)
                    news_df = load_company_news(selected_symbol)
                    if not news_df.empty:
                        for index, row in news_df.iterrows():
                            st.markdown(f"**{row['news_date']}** - [{row['headline']}]({row['url']}) ({row['source']})")
                    else:
                        st.write("No news available for this company.")

                # 🔹 **Stock Price History Table**
                df_display = df.drop(columns=["unix_timestamp", "v", "SMA_14", "EMA_14", "RSI", "Bollinger_Upper", "Bollinger_Lower"], errors='ignore')
                
                st.write("### Stock Price History")
                st.dataframe(df_display.head(10), hide_index=True)  # Display the last 10 days

                # 🔹 **Technical Indicators Section**
                st.subheader("Technical Indicators")
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Simple Moving Average (SMA)**")
                    fig_sma = px.line(df, x=df["trade_date"], y=["closing_price", "SMA_14"], title="SMA Indicator")
                    fig_sma.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_sma, use_container_width=True)

                    st.write("**Relative Strength Index (RSI)**")
                    fig_rsi = px.line(df, x=df["trade_date"], y="RSI", title="RSI Indicator")
                    fig_rsi.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_rsi, use_container_width=True)

                with col2:
                    st.write("**Exponential Moving Average (EMA)**")
                    fig_ema = px.line(df, x=df["trade_date"], y=["closing_price", "EMA_14"], title="EMA Indicator")
                    fig_ema.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_ema, use_container_width=True)

                    st.write("**Bollinger Bands**")
                    fig_bb = px.line(df, x=df["trade_date"], y=["closing_price", "Bollinger_Upper", "Bollinger_Lower"], title="Bollinger Bands")
                    fig_bb.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_bb, use_container_width=True)
            else:
                st.warning("No data available for the selected company.")


elif selected_page == "Stock Comparison":
    # MULTI-STOCK COMPARISON SECTION
    st.title("Compare Two Stocks")
    
    company_options = load_company_list()
    
    if company_options:
        company_labels = [company["dropdown_label"] for company in company_options]
        col1, col2 = st.columns(2)
        
        st.subheader("Technical Indicators")
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Simple Moving Average (SMA)**")
            fig_sma = px.line(df, x=df["trade_date"], y=["closing_price", "SMA_14"], title="SMA Indicator")
            fig_sma.update_xaxes(tickformat="%Y-%m-%d")
            st.plotly_chart(fig_sma, use_container_width=True)

            st.write("**Relative Strength Index (RSI)**")
            fig_rsi = px.line(df, x=df["trade_date"], y="RSI", title="RSI Indicator")
            fig_rsi.update_xaxes(tickformat="%Y-%m-%d")
            st.plotly_chart(fig_rsi, use_container_width=True)

        with col2:
            st.write("**Exponential Moving Average (EMA)**")
            fig_ema = px.line(df, x=df["trade_date"], y=["closing_price", "EMA_14"], title="EMA Indicator")
            fig_ema.update_xaxes(tickformat="%Y-%m-%d")
            st.plotly_chart(fig_ema, use_container_width=True)

            st.write("**Bollinger Bands**")
            fig_bb = px.line(df, x=df["trade_date"], y=["closing_price", "Upper Band", "Lower Band"], title="Bollinger Bands")
            fig_bb.update_xaxes(tickformat="%Y-%m-%d")
            st.plotly_chart(fig_bb, use_container_width=True)
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

