import requests
import pandas as pd
import sqlite3
import os
import time
import config  # Import the config module

# Use the API key from config.py
API_KEY = config.API_KEY
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY,
}

# Base URL for the CoinMarketCap API
base_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical'

# Define the tickers you want to pull data for
tickers = {
    'Star Atlas (Utility)': 'ATLAS',
    'Star Atlas (Governance)': 'POLIS',
    'Render Network': 'RNDR',
    'PsyOptions': 'PSY',
    'Orca': 'ORCA',
    'Helium (HIP 70)': 'HNT',
    'Solana (Solana Labs)': 'SOL',
    'Mango Markets': 'MNGO',
    'Saber': 'SBR',
    'Raydium': 'RAY',
    'Bonk': 'BONK'
}

# Define the start and end dates for the historical data (12 months)
end_date = pd.Timestamp.now()
start_date = end_date - pd.DateOffset(years=1)

# SQLite database connection
conn = sqlite3.connect('crypto_data.db')
c = conn.cursor()

# Function to fetch historical data
def fetch_historical_data(symbol, start_date, end_date):
    time.sleep(15)  # Delay to avoid hitting rate limits
    url = f'{base_url}?symbol={symbol}&time_start={int(start_date.timestamp())}&time_end={int(end_date.timestamp())}&interval=daily'
    response = requests.get(url, headers=headers)
    data = response.json()

    if 'data' in data:
        ohlcv = data['data']['quotes']
        df = pd.DataFrame(ohlcv)
        return df
    else:
        print(f"Error fetching data for {symbol}: {data}")
        return None

# Function to store data in SQLite
def store_data_in_sqlite(df, symbol):
    df.to_sql(symbol, conn, if_exists='replace', index=False)
    print(f"Data for {symbol} stored in SQLite database.")

# Function to store data as Parquet
def store_data_as_parquet(df, symbol):
    parquet_filename = f"{symbol}_historical_data.parquet"
    df.to_parquet(parquet_filename, index=False)
    print(f"Data for {symbol} stored as Parquet file: {parquet_filename}")

# Function to load data from SQLite
def load_data_from_sqlite(symbol):
    query = f"SELECT * FROM {symbol}"
    df = pd.read_sql(query, conn)
    return df

# Function to load data from Parquet
def load_data_from_parquet(symbol):
    parquet_filename = f"{symbol}_historical_data.parquet"
    if os.path.exists(parquet_filename):
        df = pd.read_parquet(parquet_filename)
        return df
    else:
        print(f"Parquet file for {symbol} not found.")
        return None

# Main data fetching and storing loop
for name, symbol in tickers.items():
    print(f"Fetching daily data for {name} ({symbol})...")
    df = fetch_historical_data(symbol, start_date, end_date)
    if df is not None:
        # Store in SQLite and Parquet
        store_data_in_sqlite(df, symbol)
        store_data_as_parquet(df, symbol)
    else:
        print(f"Failed to fetch data for {name}.")

print("Daily data fetching and storing complete.")

# Example of loading data from SQLite and Parquet
# Replace 'ATLAS' with the symbol you want to load
example_symbol = 'ATLAS'
df_sqlite = load_data_from_sqlite(example_symbol)
df_parquet = load_data_from_parquet(example_symbol)

# Display the first few rows of data from SQLite and Parquet
print(f"Data from SQLite for {example_symbol}:\n", df_sqlite.head())
print(f"Data from Parquet for {example_symbol}:\n", df_parquet.head())

# Close the SQLite connection
conn.close()

