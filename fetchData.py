import requests
import pandas as pd
import sqlite3
import time
import config  # Import the config module

# Use the API key from config.py
API_KEY = config.API_KEY
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': API_KEY,
}

# Base URL for the CoinMarketCap API
base_url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/historical'

# Define the tickers you want to pull data for
tickers = {
    'Star Atlas (Utility)': 'ATLAS',
    'Star Atlas (Governance)': 'POLIS',
    'PsyOptions': 'PSY',
    'Orca': 'ORCA',
    'Helium (HIP 70)': 'HNT',
    'Solana (Solana Labs)': 'SOL',
    'Mango Markets': 'MNGO',
    'Saber': 'SBR',
    'Raydium': 'RAY',
    'Bonk': 'BONK'
}

# Define the start and end dates for the historical data
fixed_start_date = pd.Timestamp('2023-10-01').tz_localize(None)
fixed_end_date = pd.Timestamp('2023-12-01').tz_localize(None)

# SQLite database connection
conn = sqlite3.connect('crypto_data.db')
c = conn.cursor()

# Function to check if a table exists
def table_exists(symbol):
    c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{symbol}';")
    return c.fetchone() is not None

# Function to fetch historical data
def fetch_historical_data(symbol, start_date, end_date):
    time.sleep(2)  # Adjust delay as needed to stay within rate limits
    url = f'{base_url}?symbol={symbol}&time_start={int(start_date.timestamp())}&time_end={int(end_date.timestamp())}&interval=daily'
    response = requests.get(url, headers=headers)
    data = response.json()

    if 'data' in data and 'quotes' in data['data']:
        ohlcv = data['data']['quotes']
        df = pd.DataFrame(ohlcv)
        
        # Flatten the nested dictionaries (if any)
        df = df.applymap(lambda x: str(x) if isinstance(x, dict) else x)
        print(f"Fetched data for {symbol}:\n{df.head()}")  # Log fetched data
        
        return df
    else:
        print(f"Error fetching data for {symbol}: {data}")
        return None

# Function to store data in SQLite
def store_data_in_sqlite(df, symbol):
    print(f"Storing data for {symbol} in the database...")  # Log storage action
    df.to_sql(symbol, conn, if_exists='replace', index=False)
    print(f"Data for {symbol} stored in SQLite database.")

# Main data fetching and storing loop
for name, symbol in tickers.items():
    if table_exists(symbol):
        # Retrieve the most recent date from the local database
        query = f"SELECT MAX(timestamp) FROM {symbol}"
        c.execute(query)
        last_date = c.fetchone()[0]
        if last_date:
            last_date = pd.to_datetime(last_date).tz_localize(None)
            start_date = last_date + pd.DateOffset(days=1)
        else:
            start_date = fixed_start_date  # Start from the fixed start date
    else:
        # If the table doesn't exist, fetch data from the fixed start date
        start_date = fixed_start_date
    
    end_date = fixed_end_date
    
    # Ensure start_date is always before end_date
    if start_date >= end_date:
        print(f"Invalid date range for {symbol}: start_date {start_date} is not before end_date {end_date}")
        continue
    
    print(f"Fetching daily data for {name} ({symbol}) from {start_date.date()} to {end_date.date()}...")
    df = fetch_historical_data(symbol, start_date, end_date)
    if df is not None:
        # Store in SQLite
        store_data_in_sqlite(df, symbol)
    else:
        print(f"Failed to fetch data for {name}.")

print("Daily data fetching and storing complete.")

# Close the SQLite connection
conn.close()






