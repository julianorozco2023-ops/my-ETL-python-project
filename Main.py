#Basic ETL pipeline (Extract, Transform, Load) pipeline
import requests
import json
import pandas as pd
from sqlalchemy import create_engine

#Extraction of data from url 
#Fetch daily stock data from AlphaVantage API
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSCO.LON&outputsize=full&apikey=demo"
response = requests.get(url)
data = response.json()

#Storing raw data 
#Save raw backup before any transformation
with open("raw_data.json", "w") as f:
    json.dump(data, f)

print("Data extracted and saved to raw_data.json")

#Transforming data (import pandas as pd)
time_series = data["Time Series (Daily)"]

df = pd.DataFrame(time_series).T
print(df)

#Promote the date index into a proper column
df.reset_index(inplace=True)
print(df)
df.rename(columns={"index": "date"}, inplace=True)
print(df)

#Rename column names to clean names
#Takes numbers off of columns names
df.rename(columns={
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume",
}, inplace=True)
print(df)

#Cast numeric columns from strings to float/ints
df["open"] = df["open"].astype(float)
df["high"] = df["high"].astype(float)
df["low"] = df["low"].astype(float)
df["close"] = df["close"].astype(float)
df["volume"] = df["volume"].astype(float)
df["date"] = pd.to_datetime(df["date"])

print(f"Transformed {len(df)} rows")
print(df.head())

#Loading into database 
engine = create_engine("sqlite:///my_database.db")
df.to_sql("stocks_prices", engine, if_exists="replace", index=False)
print("Data loaded into SQLite -> table: stocks_prices")

#SQL

#Basic Queries

#View most recent 10 days
pd.read_sql("SELECT * FROM stocks_prices ORDER BY date DESC LIMIT 10", engine)

#Filter a date range
pd.read_sql("""
    SELECT * FROM stocks_prices
    WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
    ORDER BY date
    """, engine)

#Aggregations

#Monthly average closing price
pd.read_sql("""
    SELECT strftime('%Y-%m', date) AS month,
           ROUND(AVG(close), 2) AS avg_close,
           MAX(high) AS monthly_high,
           MIN(low) AS monthly_low
    FROM stocks_prices
    GROUP BY month
    ORDER BY month DESC 
""", engine)

#Analysis Queries

#Biggest single-day price swings
pd.read_sql("""
    SELECT date,
            ROUND(high - low, 2) AS daily_range,
            close
    FROM stocks_prices
    ORDER BY daily_range DESC
    LIMIT 10
""", engine)

#Days where price closed higher than it opened
pd.read_sql("""
    SELECT COUNT(*) AS bullish_days
    FROM stocks_prices
    WHERE close > open
""", engine)

#Highest volume days
print(pd.read_sql("""
    SELECT date, volume, close 
    FROM stocks_prices
    ORDER BY volume DESC
    LIMIT 10
""", engine))
