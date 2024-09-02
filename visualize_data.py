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

# Initialize an empty DataFrame to hold all the percentage changes
all_pct_changes = pd.DataFrame()

# Loop through each ticker and fetch the data
for ticker in tickers:
    try:
        query = f"SELECT timestamp, quote FROM {ticker} ORDER BY timestamp"
        df = pd.read_sql(query, conn, parse_dates=['timestamp'])
        print(f"Data for {ticker}:\n", df.head())  # Debug: Check data retrieval
        
        # Extract the closing price from the 'quote' dictionary
        df['close'] = df['quote'].apply(lambda x: ast.literal_eval(x)['USD']['price'])
        
        # Calculate the percentage change day-over-day
        df['pct_change'] = df['close'].pct_change() * 100  # Convert to percentage
        print(f"Percentage changes for {ticker}:\n", df['pct_change'].head())  # Debug: Check percentage calculation
        
        # Add the percentage change to the all_pct_changes DataFrame
        df.set_index('timestamp', inplace=True)
        all_pct_changes[ticker] = df['pct_change']
    
    except pd.io.sql.DatabaseError:
        print(f"No table found for {ticker}. Skipping...")
        continue
    
    except KeyError as e:
        print(f"KeyError for {ticker}: {e}. Skipping this token.")
        continue

# Debug: Check the combined DataFrame before plotting
print("Combined percentage changes data:\n", all_pct_changes.head())

# Plotting the data
plt.figure(figsize=(14, 8))
if not all_pct_changes.empty:
    for ticker in all_pct_changes.columns:
        plt.plot(all_pct_changes.index, all_pct_changes[ticker], label=ticker)
    
    # Format the x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))  # Show every 5th day
    plt.gcf().autofmt_xdate()  # Rotate the x-axis dates for better readability
    
    plt.title('Daily % Change in Close Price for Cryptocurrencies')
    plt.xlabel('Date')
    plt.ylabel('% Change')
    plt.legend(loc='upper left')
    plt.grid(True)
    
    # Save the figure to the main directory
    plt.savefig('daily_pct_change_cryptos.png')
    
    # Show the plot
    plt.show()
else:
    print("No data available to plot.")

# Close the SQLite connection
conn.close()







