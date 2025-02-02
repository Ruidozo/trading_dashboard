import pandas as pd

parquet_file = "/tmp/daily_stocks_2025-02-01.parquet"  # Adjust the filename as needed
df = pd.read_parquet(parquet_file, engine="pyarrow")

print(df.columns)  # Check if "v" exists
print(df.head())   # Preview the data
