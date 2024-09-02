import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('crypto_data.db')
c = conn.cursor()

# List all tables
c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()
print("Tables in the database:", tables)

# Preview the first few rows of each table
for table in tables:
    table_name = table[0]
    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn)
    print(f"\nData from {table_name}:\n", df)

conn.close()
