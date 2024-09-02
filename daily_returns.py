import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import ast
import matplotlib.dates as mdates
from matplotlib.patches import Patch

# Connect to the SQLite database
conn = sqlite3.connect('crypto_data.db')

# Define the tickers you want to visualize
tickers = [
    'ATLAS', 'POLIS', 'RNDR', 'PSY', 'ORCA',
    'HNT', 'SOL', 'MNGO', 'SBR', 'RAY', 'BONK'
]

# Initialize an empty DataFrame to hold all the daily returns
all_daily_returns = pd.DataFrame()

# Loop through each ticker and fetch the data
for ticker in tickers:
    try:
        query = f"SELECT timestamp, quote FROM {ticker} ORDER BY timestamp"
        df = pd.read_sql(query, conn, parse_dates=['timestamp'])
        
        # Extract the closing price from the 'quote' dictionary
        df['close'] = df['quote'].apply(lambda x: ast.literal_eval(x)['USD']['price'])
        
        # Calculate the daily returns
        df['daily_return'] = df['close'].pct_change() * 100  # Daily return as a percentage
        
        # Add the daily returns to the all_daily_returns DataFrame
        df.set_index('timestamp', inplace=True)
        all_daily_returns[ticker] = df['daily_return']
    
    except pd.io.sql.DatabaseError:
        print(f"No table found for {ticker}. Skipping...")
        continue
    
    except KeyError as e:
        print(f"KeyError for {ticker}: {e}. Skipping this token.")
        continue

# Drop rows with NaN values (which occur due to the pct_change calculation)
all_daily_returns.dropna(inplace=True)

# Filter the DataFrame for the desired date range
start_date = '2023-10-27'
end_date = '2023-11-06'
filtered_daily_returns = all_daily_returns.loc[start_date:end_date]

# Creating the daily returns plot
plt.figure(figsize=(14, 8))
for ticker in filtered_daily_returns.columns:
    plt.plot(filtered_daily_returns.index, filtered_daily_returns[ticker], label=ticker)

# Highlight the Breakpoint time period (Oct 30 to Nov 3)
plt.axvspan('2023-10-30', '2023-11-03', color='lightgray', alpha=0.5)

# Adding a custom legend for the Breakpoint shading
custom_legend = [Patch(color='lightgray', alpha=0.5, label='Breakpoint Period')]

# Format the x-axis dates for better readability
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day in the range
plt.gcf().autofmt_xdate()  # Rotate the x-axis dates for better readability

plt.title('Daily Returns of Cryptocurrencies (Oct 27, 2023 - Nov 6, 2023)')
plt.xlabel('Date')
plt.ylabel('Daily Return (%)')
plt.legend(loc='upper left')

# Combine the custom legend with the existing one
plt.legend(handles=plt.gca().get_legend_handles_labels()[0] + custom_legend, loc='upper left')

plt.grid(True)

# Save the plot to the main directory
plt.savefig('crypto_daily_returns_with_breakpoint.png')

# Show the plot
plt.show()

# Close the SQLite connection
conn.close()
