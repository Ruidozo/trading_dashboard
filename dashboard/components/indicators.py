import pandas as pd

def calculate_sma(df, window=14):
    """Calculate Simple Moving Average (SMA)."""
    if "closing_price" in df.columns:
        df[f"SMA_{window}"] = df["closing_price"].rolling(window=window, min_periods=1).mean()
    else:
        print("Error: closing_price column is missing in DataFrame")
    return df


def calculate_ema(df, window=14):
    """Calculate Exponential Moving Average (EMA)."""
    df[f"EMA_{window}"] = df["closing_price"].ewm(span=window, adjust=False).mean()
    return df

def calculate_rsi(df, window=14):
    """Calculate Relative Strength Index (RSI)."""
    delta = df["closing_price"].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

def calculate_bollinger_bands(df, window=20):
    """Calculate Bollinger Bands."""
    df["SMA_20"] = df["closing_price"].rolling(window=window).mean()
    df["Upper Band"] = df["SMA_20"] + (df["closing_price"].rolling(window=window).std() * 2)
    df["Lower Band"] = df["SMA_20"] - (df["closing_price"].rolling(window=window).std() * 2)
    return df
