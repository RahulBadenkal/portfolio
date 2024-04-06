import json
from datetime import datetime

import pytz
import yfinance as yf
import pandas as pd


def get_info():
    # Fetch data for a specific ticker
    ticker = yf.Ticker("HDFC")

    # Calling history automatically downloads info data
    ticker.history(interval="1d", start='1980-04-06', end='1980-04-07')

    t1 = datetime.now()
    x = ticker.info
    # print(json.dumps(ticker.info, indent=2, default=str))
    t2 = datetime.now()
    print("took", t2 - t1)


def get_market_cap():
    # Fetch data for a specific ticker
    ticker = yf.Ticker("AAPL")

    # Get market capitalization
    market_cap = ticker.info['marketCap']

    print(f"Market Cap: {market_cap}")


def get_listed_on():
    # Fetch data for a specific ticker
    ticker = yf.Ticker("AAPL")

    # Get market capitalization
    listed_on = ticker.info['firstTradeDateEpochUtc']
    listed_on = datetime.utcfromtimestamp(listed_on)
    listed_on = listed_on.replace(tzinfo=pytz.UTC)

    print(ticker.info['firstTradeDateEpochUtc'])
    print(f"Listed on: {listed_on}")



def get_day_info():
    # Define the ticker symbol
    tickerSymbol = 'AAPL'

    # Get data on this ticker
    tickerData = yf.Ticker(tickerSymbol)

    # Get the historical prices for this ticker
    tickerDf = tickerData.history(interval="1d", start='2024-04-06', end='2024-04-07')

    # Print the data
    pd.set_option('display.max_columns', None)
    print(tickerDf)


def get_ticker_currency():
    # Define the ticker symbol
    tickerSymbol = 'AAPL'

    # Get data on this ticker
    tickerData = yf.Ticker(tickerSymbol)

    # Access the currency information
    currency = tickerData.info['currency']

    print(f"The currency for {tickerSymbol} is {currency}.")


def get_ticker_details(tickerSymbol):
    # Get data on this ticker
    tickerData = yf.Ticker(tickerSymbol)

    # Access the ticker information
    tickerInfo = tickerData.info

    # Print some details about the company
    print(f"Company Name: {tickerInfo['longName']}")
    print(f"Sector: {tickerInfo['sector']}")
    print(f"Industry: {tickerInfo['industry']}")
    print(f"Exchange: {tickerInfo['exchange']}")
    print(f"Website: {tickerInfo['website']}")


# msft = yf.Ticker("MSFT")
# print(msft)
get_info()
# get_day_info()