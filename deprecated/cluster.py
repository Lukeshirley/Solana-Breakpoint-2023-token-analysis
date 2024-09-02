import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import ast
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import seaborn as sns

# Connect to the SQLite database
conn = sqlite3.connect('crypto_data.db')

# Define the tickers you want to cluster
tickers = [
    'ATLAS', 'POLIS', 'RNDR', 'PSY', 'ORCA',
    'HNT', 'SOL', 'MNGO', 'SBR', 'RAY', 'BONK'
]

# Initialize an empty DataFrame to hold the daily returns
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

# Standardize the data
scaler = StandardScaler()
scaled_returns = scaler.fit_transform(all_returns)

# Apply K-means clustering
kmeans = KMeans(n_clusters=3, random_state=42)  # Change n_clusters to adjust the number of clusters
clusters = kmeans.fit_predict(scaled_returns)

# Add the cluster labels to the DataFrame
all_returns['Cluster'] = clusters

# Plotting the clusters
plt.figure(figsize=(12, 8))
sns.scatterplot(x=all_returns.index, y='Cluster', hue='Cluster', palette='viridis', data=all_returns, legend='full')
plt.title('K-Means Clustering of Cryptocurrencies Based on Daily Returns')
plt.xlabel('Date')
plt.ylabel('Cluster')
plt.legend(title='Cluster')
plt.grid(True)

# Save the plot to the main directory
plt.savefig('crypto_kmeans_clusters.png')

# Show the plot
plt.show()

# Close the SQLite connection
conn.close()
