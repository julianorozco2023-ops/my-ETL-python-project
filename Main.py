#Basic ETL pipeline (Extract, Transform, Load) pipeline
import requests
import json
import pandas as pd
from sqlalchemy import create_engine

#Config variables
SYMBOL = "TSCO.LON"
API_KEY = "demo"
DB_PATH = "sqlite:///my_database.db"
TABLE = "stocks_prices"

#Extraction of data from url 
#Fetch daily stock data from AlphaVantage API
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&outputsize=full&apikey={API_KEY}"
response = requests.get(url)
response.raise_for_status()
data = response.json()

if "Time Series (Daily)" not in data:
    raise KeyError(f"Unexpected API response: {data}")

#Storing raw data 
#Save raw backup before any transformation
with open("raw_data.json", "w") as f:
    json.dump(data, f)

print("Data extracted and saved to raw_data.json")

#Transforming data
time_series = data["Time Series (Daily)"]

df = pd.DataFrame(time_series).T

#Promote the date index into a proper column
df.reset_index(inplace=True)

#Rename column names to clean names
#Takes numbers off of columns names
df.rename(columns={
    "index" : "date",
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume",
}, inplace=True)

#Cast numeric columns from strings to float/ints
for col in ["open", "high", "low", "close", "volume"]:
    df[col] = df[col].astype(float)
df["date"] = pd.to_datetime(df["date"])

print(f"Transformed {len(df)} rows")
print(df.head())

#Loading into database 
engine = create_engine(DB_PATH)
df.to_sql(TABLE, engine, if_exists="replace", index=False)
print(f"Data loaded into SQLite -> table: {TABLE}")

#SQL

#Basic Queries

#View most recent 10 days
df_recent_days = pd.read_sql(f"SELECT * FROM {TABLE} ORDER BY date DESC LIMIT 10", engine)

#Filter a date range
df_data_range = pd.read_sql(f"""
    SELECT * FROM {TABLE}
    WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
    ORDER BY date
    """, engine)

#Aggregations

#Monthly average closing price
df_mothly_close_price = pd.read_sql(f"""
    SELECT strftime('%Y-%m', date) AS month,
           ROUND(AVG(close), 2) AS avg_close,
           MAX(high) AS monthly_high,
           MIN(low) AS monthly_low
    FROM {TABLE}
    GROUP BY month
    ORDER BY month DESC 
""", engine)

#Analysis Queries

#Biggest single-day price swings
df_highest_singleday_price_swing = pd.read_sql(f"""
    SELECT date,
            ROUND(high - low, 2) AS daily_range,
            close
    FROM {TABLE}
    ORDER BY daily_range DESC
    LIMIT 10
""", engine)

#Days where price closed higher than it opened
df_close_higher_than_open = pd.read_sql(f"""
    SELECT COUNT(*) AS bullish_days
    FROM {TABLE}
    WHERE close > open
""", engine)

#Highest volume days
df_highest_volume = pd.read_sql(f"""
    SELECT date, volume, close 
    FROM {TABLE}
    ORDER BY volume DESC
    LIMIT 10
""", engine)

print(df_highest_volume)
print(df_close_higher_than_open)
