from datetime import datetime

import pytz
import yfinance as yf
import pandas as pd


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
    tickerDf = tickerData.history(period='1m', start='2022-01-02', end='2024-01-03')

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


get_day_info()