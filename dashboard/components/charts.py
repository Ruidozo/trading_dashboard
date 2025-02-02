import plotly.graph_objects as go

def plot_dark_candlestick_chart(df, ticker):
    """Generates a dark-mode candlestick chart with moving average."""
    fig = go.Figure()

    # Candlestick Chart
    fig.add_trace(go.Candlestick(
        x=df["trade_date"],
        open=df["opening_price"],
        high=df["highest_price"],
        low=df["lowest_price"],
        close=df["closing_price"],
        name="OHLC",
        increasing_line_color='green',
        decreasing_line_color='red'
    ))

    # Moving Average (7-day)
    fig.add_trace(go.Scatter(
        x=df["trade_date"],
        y=df["closing_price"].rolling(window=7).mean(),
        mode="lines",
        name="7-Day MA",
        line=dict(dash="dot", color="white")
    ))

    fig.update_layout(
        template="plotly_dark",
        title=f"{ticker} Stock Performance",
        xaxis_title="Date",
        yaxis_title="Price",
        font=dict(color="white"),
        xaxis=dict(gridcolor="gray"),
        yaxis=dict(gridcolor="gray"),
        margin=dict(l=0, r=0, t=40, b=20),
        height=350
    )

    return fig
