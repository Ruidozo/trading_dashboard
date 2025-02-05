import plotly.graph_objects as go

def plot_dark_candlestick_chart(df, company_name):
    """Generate a professional candlestick chart using OHLC data."""
    fig = go.Figure(data=[go.Candlestick(
        x=df["trade_date"],
        open=df["opening_price"],
        high=df["highest_price"],
        low=df["lowest_price"],
        close=df["closing_price"],
        increasing_line_color='green', 
        decreasing_line_color='red'
    )])

    fig.update_layout(
        title=f"{company_name} Stock Price Movement",
        xaxis_title="Date",
        yaxis_title="Price",
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=14),
        hovermode="x",
        xaxis=dict(
            showline=True,
            showgrid=True,
            gridcolor="lightgray",
            tickformat="%Y-%m-%d"
        ),
        yaxis=dict(
            showline=True,
            showgrid=True,
            gridcolor="lightgray"
        )
    )
    return fig
