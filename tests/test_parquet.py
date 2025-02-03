import pandas as pd

parquet_file = "2025_02_daily_stocks_2025-02-03.parquet"  # Adjust the filename as needed
df = pd.read_parquet(parquet_file, engine="pyarrow")

print(df.columns)  # Check if "v" exists
print(df.head())   # Preview the data
