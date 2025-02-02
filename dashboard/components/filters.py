import pandas as pd

def compute_sma(df, window=7):
    """Calculate Simple Moving Average (SMA)."""
    df[f"SMA_{window}"] = df["closing_price"].rolling(window=window).mean()
    return df

def compute_ema(df, window=7):
    """Calculate Exponential Moving Average (EMA)."""
    df[f"EMA_{window}"] = df["closing_price"].ewm(span=window, adjust=False).mean()
    return df

def compute_rsi(df, window=14):
    """Calculate Relative Strength Index (RSI)."""
    delta = df["closing_price"].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

def compute_bollinger_bands(df, window=20, num_std=2):
    """Calculate Bollinger Bands."""
    rolling_mean = df["closing
