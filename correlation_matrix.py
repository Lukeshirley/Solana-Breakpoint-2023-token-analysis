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

# Initialize an empty DataFrame to hold all the daily returns
all_returns = pd.DataFrame()

# Loop through each ticker and fetch the data
for ticker in tickers:
    try:
        query = f"SELECT timestamp, quote FROM {ticker} ORDER BY timestamp"
        df = pd.read_sql(query, conn, parse_dates=['timestamp'])
        
        # Extract the closing price from the 'quote' dictionary
        df['close'] = df['quote'].apply(lambda x: ast.literal_eval(x)['USD']['price'])
        
        # Calculate the daily returns
        df['daily_return'] = df['close'].pct_change()  # Daily return as a decimal
        
        # Add the daily returns to the all_returns DataFrame
        df.set_index('timestamp', inplace=True)
        all_returns[ticker] = df['daily_return']
    
    except pd.io.sql.DatabaseError:
        print(f"No table found for {ticker}. Skipping...")
        continue
    
    except KeyError as e:
        print(f"KeyError for {ticker}: {e}. Skipping this token.")
        continue

# Drop rows with NaN values (which occur due to the pct_change calculation)
all_returns.dropna(inplace=True)

# Calculate the correlation matrix
correlation_matrix = all_returns.corr()

# Creating the heatmap for the correlation matrix
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0, linewidths=.5)
plt.title('Correlation Matrix of Daily Returns for Cryptocurrencies')
plt.xlabel('Token')
plt.ylabel('Token')
plt.tight_layout()

# Save the heatmap to the main directory
plt.savefig('crypto_correlation_matrix.png')

# Show the heatmap
plt.show()

# Close the SQLite connection
conn.close()
