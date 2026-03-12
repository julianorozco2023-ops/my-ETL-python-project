#Basic ETL pipeline (Extract, Transform, Load) pipeline
import requests
import json
import pandas as pd
from sqlalchemy import create_engine

#Extraction of data from url (import requests)
#Fetch daily stock data from AlphaVantage API
#requests.get, sends GET request to a url and returns a response object
#response.json, converts web api's json response body into a native python object, such as a dictionary or a list
url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=TSCO.LON&outputsize=full&apikey=demo"
response = requests.get(url)
data = response.json()

#Storing raw data (import json)
#Save raw backup before any transformation
#open, opens a file, establishes connection between the program and file and return a corresponding file object
#w opens a file in write mode, erases the entire file and rewrites
#a opens a file in append mode, meaning adding to the current file on hand
#json is javascript object notation, used for structuring, storing and transporting data
#json.dump used to serialize a python object into a JSON formatted stream and write it directly to a file
with open("raw_data.json", "w") as f:
    json.dump(data, f)

print("Data extracted and saved to raw_data.json")

#Transforming data (import pandas as pd)
#Extract fields that I care about, convert timestamps, rename columns, handle missing values
#pd.DataFrame is a constructor, creates a two-dimensional, size-mutable, tabular data structure with labeled rows and columns
#can be moved to a spreadsheet, sql table, etc.
#df = dataframe
#For 1 column of extraction only use 1 bracket, for multiple use 2

#Pull out just the series dict {date: {open, high, low, close, volume}}
time_series = data["Time Series (Daily)"]

#Use pd.DataFrame(ts).T to correctly orient data
#Without .T each *date* becomes a column - wrong structure
#With .T each *date* becomes a row - correct structure
df = pd.DataFrame(time_series).T
print(df)

#Promote the date index into a proper column
#inplace, operations that modify the original object directly, without creating a new copy
#This function adds the index and shifts the date over
#df.reset_index(), used to reset the index of a df to default int index (0, 1, 2, ...). The original index is added as a new column in the df
#df.rename(), used to properly name the "date" column
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
#pd.to_datetime, converts strings, integers, floats, or even a df / dict-like object into a pandas datetime object. Essential for working with time-series data
#len{df} returns number of rows
df["open"] = df["open"].astype(float)
df["high"] = df["high"].astype(float)
df["low"] = df["low"].astype(float)
df["close"] = df["close"].astype(float)
df["volume"] = df["volume"].astype(float)
df["date"] = pd.to_datetime(df["date"])

print(f"Transformed {len(df)} rows")
print(df.head())

#Loading into database (from sqlalchemy import create_engine)
#create_engine() is a function in python sqlalchemy library used to establish connectivity to a database. Returns engine instance, handles communication between python application and database
#df.to_sql is used to write what's stored in the dataframe to a sql database table.
#if_exists = replace, drops the existing table entirely and recreates from scratch on the df's current structure. Built new table with all the right columns before inserting
engine = create_engine("sqlite:///my_database.db")
df.to_sql("stocks_prices", engine, if_exists="replace", index=False)
print("Data loaded into SQLite -> table: stocks_prices")

#SQL

#Basic Queries

#View most recent 10 days
#SELECT, command used to retrieve data from one or more database tables
#AS, assign a temporary name (alias) to a column or a table during the execution of a query
#FROM, used to specify the source table or tables from which data should be retrieved or manipulated
#WHERE, used to filter records and extract only those rows that fulfill a specified condition
#ORDER, used to sort the result set of a SELECT query in ascending (ASC) or descending (DESC) order based on one or more columns
#* is a wildcard for all columns
#pd.read_sql, function that is used to read data from a sql database directly into a pandas df
pd.read_sql("SELECT * FROM stocks_prices ORDER BY date DESC LIMIT 10", engine)

#Filter a date range
pd.read_sql("""
    SELECT * FROM stocks_prices
    WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
    ORDER BY date
    """, engine)

#Aggregations

#Monthly average closing price
#strftime() "string format time", converts date, time, or datetime objects into human-readable formatted string.
#%y, Represents the year without the century. (ex: 26 for 2026)
#-, A literal hyphen separator
#%m Represents the month as a zero-padded decimal. (ex: 03 for march)
#ROUND(), function used to round a number to a specified number of decimal places or nearest integer
#AVG(), returns average (arithmetic mean) value of a specified numeric column
#close, this is the column name
#high, this is the column name
#low, this is the column name
#MAX(), an aggregate function that returns the largest (maximum) value of a specified column. Could be for numeric, string and date data types
#MIN(), aggregate function that returns the smallest value from a specified column
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
#Gets date, subtracts the high from the low to get difference and orders them in descending order from largest to smallest change
pd.read_sql("""
    SELECT date,
            ROUND(high - low, 2) AS daily_range,
            close
    FROM stocks_prices
    ORDER BY daily_range DESC
    LIMIT 10
""", engine)

#Days where price closed higher than it opened
#COUNT(*), is an aggregate function that returns the total number of rows in a table or a query's result set. Including rows that contain null or duplicate values
#Sets the count as bullish_days, from stock_prices, returns whether the close value was greater than the open value for however many the counter counted
pd.read_sql("""
    SELECT COUNT(*) AS bullish_days
    FROM stocks_prices
    WHERE close > open
""", engine)

#Highest volume days
#Gets the date, volume and close values, from stocks_prices, orders them in descending order based on the volume
print(pd.read_sql("""
    SELECT date, volume, close 
    FROM stocks_prices
    ORDER BY volume DESC
    LIMIT 10
""", engine))