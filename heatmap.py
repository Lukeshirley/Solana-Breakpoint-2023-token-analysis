import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import ast

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
        
        # Extract the closing price from the 'quote' dictionary
        df['close'] = df['quote'].apply(lambda x: ast.literal_eval(x)['USD']['price'])
        
        # Calculate the percentage change day-over-day
        df['pct_change'] = df['close'].pct_change() * 100  # Convert to percentage
        
        # Add the percentage change to the all_pct_changes DataFrame
        df.set_index('timestamp', inplace=True)
        all_pct_changes[ticker] = df['pct_change']
    
    except pd.io.sql.DatabaseError:
        print(f"No table found for {ticker}. Skipping...")
        continue
    
    except KeyError as e:
        print(f"KeyError for {ticker}: {e}. Skipping this token.")
        continue

# Drop rows with NaN values (which occur due to the pct_change calculation)
all_pct_changes.dropna(inplace=True)

# Creating the heatmap
plt.figure(figsize=(16, 10))
sns.heatmap(all_pct_changes.T, cmap='RdYlGn', center=0, annot=False, linewidths=.5, cbar_kws={'label': '% Change'})
plt.title('Heatmap of Daily % Change in Close Price for Cryptocurrencies')
plt.xlabel('Date')
plt.ylabel('Token')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

# Save the heatmap to the main directory
plt.savefig('crypto_daily_pct_change_heatmap.png')

# Show the heatmap
plt.show()

# Close the SQLite connection
conn.close()
