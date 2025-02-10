from json import load
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
from utils.data_loader import (
    load_company_list,
    load_stock_data,
    load_stock_predictions,
    load_top_gainers,
    load_top_losers,
    load_trading_patterns,
    load_company_news,
    load_market_behavior,
    load_high_volatility_stocks,
    load_trading_patterns
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
                    from datetime import date
                    current_date = date.today().strftime('%Y-%m-%d')
                    predicted_price = prediction_df.iloc[0]['predicted_closing_price']
                    st.markdown(
                        f"""
                        <div style="text-align: center; padding: 10px; border: 2px solid gray; border-radius: 8px;">
                            <h3>Stock Prediction</h3>
                            <p><b>{current_date}</b></p>
                            <p><h2>{predicted_price:.3f}</h2></p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

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

if selected_page == "General Market Trends":
    # 🌍 GENERAL MARKET TRENDS SECTION
    st.title("General Market Overview")

     # 📊 General Market Behavior
    st.subheader("General Market Behavior")
    df_market_behavior = load_market_behavior()
    
    if not df_market_behavior.empty:
        fig_market_behavior = px.line(df_market_behavior, x="Date", y="Average Price", title="Daily Average Closing Price")
        fig_market_behavior.update_xaxes(tickformat="%Y-%m-%d")
        st.plotly_chart(fig_market_behavior, use_container_width=True)
    else:
        st.warning("No market behavior data available.")

    # 📊 Top Market Gainers & Losers
    st.subheader("Top Market Gainers & Losers")
    col1, col2 = st.columns(2)

    with col1:
        st.write("**Top Gainers**")
        df_gainers_losers = load_top_gainers()
        if not df_gainers_losers.empty:
            st.dataframe(df_gainers_losers)
        else:
            st.warning("No market data available.")

    with col2:
        st.write("**Top Losers**")
        df_top_losers = load_top_losers()
        if not df_top_losers.empty:
            st.dataframe(df_top_losers)
        else:
            st.warning("No market data available.")

    # 📈 High Volatility Stocks
    with col1:
        st.subheader("High Volatility Stocks")
        df_high_volatility = load_high_volatility_stocks()
        
        if not df_high_volatility.empty:
            st.dataframe(df_high_volatility)
        else:
            st.warning("No high volatility data available.")

    with col2:
    # 📋 Trading Patterns
        st.subheader("Recent Trading Patterns")
        df_trading_patterns = load_trading_patterns()
        
        if not df_trading_patterns.empty:
            st.dataframe(df_trading_patterns)
        else:
            st.warning("No trading patterns data available.")


if selected_page == "Stock Comparison":
    # MULTI-STOCK COMPARISON SECTION
    st.title("Compare Two Stocks")
    
    company_options = load_company_list()
    
    if company_options:
        company_labels = [company["dropdown_label"] for company in company_options]
        col1, col2 = st.columns(2)
        
        with col1:
            selected_label1 = st.selectbox("Select First Company", company_labels)
            selected_company1 = next((c for c in company_options if c["dropdown_label"] == selected_label1), None)
        
        with col2:
            selected_label2 = st.selectbox("Select Second Company", company_labels)
            selected_company2 = next((c for c in company_options if c["dropdown_label"] == selected_label2), None)
        
        if selected_company1 and selected_company2:
            selected_company_name1 = selected_company1["company_name"]
            selected_symbol1 = selected_company1["symbol"]
            selected_company_name2 = selected_company2["company_name"]
            selected_symbol2 = selected_company2["symbol"]

            # Load Stock Data for both companies
            end_date = date.today()
            start_date = end_date - timedelta(days=90)  # Default to last 90 days

            df1 = load_stock_data(company_name=selected_company_name1, start_date=start_date, end_date=end_date)
            df2 = load_stock_data(company_name=selected_company_name2, start_date=start_date, end_date=end_date)

            if not df1.empty and not df2.empty:
                df1 = df1.sort_values(by="trade_date", ascending=False)  # Sort by date in descending order
                df1 = calculate_sma(df1, window=14)
                df1 = calculate_ema(df1, window=14)
                df1 = calculate_rsi(df1, window=14)
                df1 = calculate_bollinger_bands(df1, window=20)

                df2 = df2.sort_values(by="trade_date", ascending=False)  # Sort by date in descending order
                df2 = calculate_sma(df2, window=14)
                df2 = calculate_ema(df2, window=14)
                df2 = calculate_rsi(df2, window=14)
                df2 = calculate_bollinger_bands(df2, window=20)

                st.subheader("Technical Indicators")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Simple Moving Average (SMA) - {selected_company_name1}**")
                    fig_sma1 = px.line(df1, x=df1["trade_date"], y=["closing_price", "SMA_14"], title=f"SMA Indicator - {selected_company_name1}")
                    fig_sma1.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_sma1, use_container_width=True, key="sma1")

                    st.write(f"**Relative Strength Index (RSI) - {selected_company_name1}**")
                    fig_rsi1 = px.line(df1, x=df1["trade_date"], y="RSI", title=f"RSI Indicator - {selected_company_name1}")
                    fig_rsi1.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_rsi1, use_container_width=True, key="rsi1")

                with col2:
                    st.write(f"**Exponential Moving Average (EMA) - {selected_company_name1}**")
                    fig_ema1 = px.line(df1, x=df1["trade_date"], y=["closing_price", "EMA_14"], title=f"EMA Indicator - {selected_company_name1}")
                    fig_ema1.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_ema1, use_container_width=True, key="ema1")

                    st.write(f"**Bollinger Bands - {selected_company_name1}**")
                    fig_bb1 = px.line(df1, x=df1["trade_date"], y=["closing_price", "Bollinger_Upper", "Bollinger_Lower"], title=f"Bollinger Bands - {selected_company_name1}")
                    fig_bb1.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_bb1, use_container_width=True, key="bb1")

                st.subheader("Technical Indicators")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Simple Moving Average (SMA) - {selected_company_name2}**")
                    fig_sma2 = px.line(df2, x=df2["trade_date"], y=["closing_price", "SMA_14"], title=f"SMA Indicator - {selected_company_name2}")
                    fig_sma2.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_sma2, use_container_width=True, key="sma2")

                    st.write(f"**Relative Strength Index (RSI) - {selected_company_name2}**")
                    fig_rsi2 = px.line(df2, x=df2["trade_date"], y="RSI", title=f"RSI Indicator - {selected_company_name2}")
                    fig_rsi2.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_rsi2, use_container_width=True, key="rsi2")

                with col2:
                    st.write(f"**Exponential Moving Average (EMA) - {selected_company_name2}**")
                    fig_ema2 = px.line(df2, x=df2["trade_date"], y=["closing_price", "EMA_14"], title=f"EMA Indicator - {selected_company_name2}")
                    fig_ema2.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_ema2, use_container_width=True, key="ema2")

                    st.write(f"**Bollinger Bands - {selected_company_name2}**")
                    fig_bb2 = px.line(df2, x=df2["trade_date"], y=["closing_price", "Bollinger_Upper", "Bollinger_Lower"], title=f"Bollinger Bands - {selected_company_name2}")
                    fig_bb2.update_xaxes(tickformat="%Y-%m-%d")
                    st.plotly_chart(fig_bb2, use_container_width=True, key="bb2")
            else:
                st.warning("No data available for one or both selected companies.")
    else:
        st.warning("No data available for one or both selected companies.")