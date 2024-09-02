import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import ast
import matplotlib.dates as mdates

# Connect to the SQLite database
conn = sqlite3.connect('crypto_data.db')

# Define the tickers you want to visualize
tickers = [
    'ATLAS', 'POLIS', 'RNDR', 'PSY', 'ORCA',
    'HNT', 'SOL', 'MNGO', 'SBR', 'RAY', 'BONK'
]

# Initialize an empty DataFrame to hold the rolling average returns
all_rolling_returns = pd.DataFrame()

# Set the window size for the rolling average
window_size = 7  # You can change this to 14 or another value for a different smoothing effect

# Loop through each ticker and fetch the data
for ticker in tickers:
    try:
        query = f"SELECT timestamp, quote FROM {ticker} ORDER BY timestamp"
        df = pd.read_sql(query, conn, parse_dates=['timestamp'])
        
        # Extract the closing price from the 'quote' dictionary
        df['close'] = df['quote'].apply(lambda x: ast.literal_eval(x)['USD']['price'])
        
        # Calculate the daily returns
        df['daily_return'] = df['close'].pct_change()  # Daily return as a decimal
        
        # Calculate the rolling average of the returns
        df['rolling_return'] = df['daily_return'].rolling(window=window_size).mean() * 100  # Convert to percentage
        
        # Add the rolling returns to the all_rolling_returns DataFrame
        df.set_index('timestamp', inplace=True)
        all_rolling_returns[ticker] = df['rolling_return']
    
    except pd.io.sql.DatabaseError:
        print(f"No table found for {ticker}. Skipping...")
        continue
    
    except KeyError as e:
        print(f"KeyError for {ticker}: {e}. Skipping this token.")
        continue

# Drop rows with NaN values (which occur due to the rolling mean calculation)
all_rolling_returns.dropna(inplace=True)

# Creating the rolling average returns plot
plt.figure(figsize=(14, 8))
for ticker in all_rolling_returns.columns:
    plt.plot(all_rolling_returns.index, all_rolling_returns[ticker], label=ticker)

# Format the x-axis dates for better readability
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))  # Show every 5th day
plt.gcf().autofmt_xdate()  # Rotate the x-axis dates for better readability

plt.title(f'{window_size}-Day Rolling Average of Daily Returns for Cryptocurrencies')
plt.xlabel('Date')
plt.ylabel(f'{window_size}-Day Rolling % Return')
plt.legend(loc='upper left')
plt.grid(True)

# Save the plot to the main directory
plt.savefig(f'crypto_{window_size}day_rolling_returns.png')

# Show the plot
plt.show()

# Close the SQLite connection
conn.close()
