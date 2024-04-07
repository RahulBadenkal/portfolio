from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import reduce
from typing import Optional, List, Literal, Any, Annotated
import httpx
import json
import exchange_calendars as xcals
import math
import pandas as pd
import yfinance as yf
from pydantic import BaseModel, AfterValidator

from common.app_utils import USA_TIMEZONE, IND_TIMEZONE
from common.httpx_utils import httpx_raise_for_status, httpx_wrapper
from common.pydantic_utils import str_validator, pos_num_check
from common.utils import csv_to_json, get_stack_trace
import logging

from stock_data_sync.config import Config


def stock_number_check(value):
    if value is not None and (math.isnan(value) or not math.isfinite(value) or value == 0):
        raise ValueError(f"Stock value can't be {value}")
    return value


class DayStockPrice(BaseModel):
    exchange_ticker: Annotated[
        str,
        AfterValidator(str_validator(trim=True, allow_empty=False)),
    ]
    ticker: Annotated[
        str,
        AfterValidator(str_validator(trim=True, allow_empty=False)),
    ]
    date: date
    currency: Annotated[
        str,
        AfterValidator(str_validator(trim=True, allow_empty=False)),
    ]
    open: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    low: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    high: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    close: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    volume: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    stock_splits: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    dividends: Annotated[
        float,
        AfterValidator(stock_number_check),
    ]
    shares: Annotated[
        Optional[float],
        AfterValidator(stock_number_check),
    ]
    market_cap: Annotated[
        Optional[float],
        AfterValidator(stock_number_check),
    ]
    pe_ratio: Annotated[
        Optional[float],
        AfterValidator(stock_number_check),
    ]
    eps: Annotated[
        Optional[float],
        AfterValidator(stock_number_check),
    ]


def extract_yticker_as_json(ticker: yf.Ticker):
    actions = ticker.actions.to_dict(orient="records")
    analyst_price_target = None  # Not implemented in yahoo finance api
    balance_sheet = ticker.balance_sheet.to_dict(orient="records")
    calendar = ticker.calendar
    capital_gains = ticker.capital_gains.to_dict()
    cash_flow = ticker.cash_flow.to_dict(orient="records")
    dividends = ticker.dividends.to_dict()
    earnings = None  # Not implemented in yahoo finance api
    earnings_date = ticker.earnings_dates.to_dict(orient="records")
    earnings_forecasts = None  # Not implemented in yahoo finance api
    earnings_trend = None  # Not implemented in yahoo finance api
    financials = ticker.financials.to_dict(orient="records")
    history_metadata = ticker.history_metadata
    income_stmt = ticker.income_stmt.to_dict(orient="records")
    info = ticker.info
    insider_purchases = ticker.insider_purchases.to_dict(orient="records")
    insider_roster_holders = ticker.insider_roster_holders.to_dict(orient="records")
    insider_transactions = ticker.insider_transactions.to_dict(orient="records")
    institutional_holders = ticker.institutional_holders.to_dict(orient="records")
    isin = ticker.isin
    major_holders = ticker.major_holders.to_dict(orient="records")
    mutualfund_holders = ticker.mutualfund_holders.to_dict(orient="records")
    news = ticker.news
    quarterly_balance_sheet = ticker.quarterly_balance_sheet.to_dict(orient="records")
    quarterly_cash_flow = ticker.quarterly_cash_flow.to_dict(orient="records")
    quarterly_earnings = None  # # Not implemented in yahoo finance api
    quarterly_financials = ticker.quarterly_financials.to_dict(orient="records")
    quarterly_income_stmt = ticker.quarterly_income_stmt.to_dict(orient="records")
    recommendations = ticker.recommendations.to_dict(orient="records")
    recommendations_summary = ticker.recommendations_summary.to_dict(orient="records")
    revenue_forecasts = None  # Not implemented in yahoo finance api
    shares = None  # Not implemented in yahoo finance api
    sustainability = None  # Not implemented in yahoo finance api
    trend_details = None  # Not implemented in yahoo finance api
    upgrades_downgrades = ticker.upgrades_downgrades.to_dict(orient="records")

    return {
        "actions": actions,
        "balance_sheet": balance_sheet,
        "calendar": calendar,
        "capital_gains": capital_gains,
        "cash_flow": cash_flow,
        "dividends": dividends,
        "earnings_date": earnings_date,
        "financials": financials,
        "history_metadata": history_metadata,
        "income_stmt": income_stmt,
        "info": info,
        "insider_purchases": insider_purchases,
        "insider_roster_holders": insider_roster_holders,
        "institutional_holders": institutional_holders,
        "insider_transactions": insider_transactions,
        "isin": isin,
        "major_holders": major_holders,
        "mutualfund_holders": mutualfund_holders,
        "news": news,
        "quarterly_balance_sheet": quarterly_balance_sheet,
        "quarterly_cash_flow": quarterly_cash_flow,
        "quarterly_financials": quarterly_financials,
        "quarterly_income_stmt": quarterly_income_stmt,
        "recommendations_summary": recommendations_summary,
        "recommendations": recommendations,
        "upgrades_downgrades": upgrades_downgrades,
    }


def scrape_ticker_for_date(exchange_ticker: str, stock_ticker: str, my_date: date, session=None, proxy=None):
    # See this link for prefixes: https://in.help.yahoo.com/kb/SLN2310.html
    yahoo_suffix, timezone = "", None
    if exchange_ticker in ["NASDAQ", "NYSE"]:
        yahoo_suffix = ""
        timezone = USA_TIMEZONE
    elif exchange_ticker == "NSE":
        yahoo_suffix = ".NS"
        timezone = IND_TIMEZONE
    else:
        raise Exception("Exchange not supported")

    # Get current date as per the exchange's timezone
    current_date = datetime.now(timezone).date()

    if my_date > current_date:
        # Future dates are not possible
        raise Exception(f"now: {current_date.strftime('%Y-%m-%d')}, provided: {my_date.strftime('%Y-%m-%d')} The provided date is in future")

    yahoo_ticker = f"{stock_ticker}{yahoo_suffix}"
    ticker = yf.Ticker(yahoo_ticker, session=session, proxy=proxy)

    # Calling history automatically downloads info data
    data = ticker.history(interval="1d", start=my_date.strftime("%Y-%m-%d"), end=(my_date + timedelta(days=1)).strftime("%Y-%m-%d")).to_dict(orient="records")[0]

    day_stock_price = DayStockPrice(
        exchange_ticker=exchange_ticker,
        ticker=stock_ticker,
        date=my_date,
        open=data.get("Open"),
        low=data.get("Low"),
        high=data.get("High"),
        close=data.get("Close"),
        volume=data.get("Volume"),
        dividends=data.get("Dividends"),
        stock_splits=data.get("Stock Splits"),
        market_cap=ticker.info.get("marketCap"),
        currency=ticker.info.get("currency"),
        shares=ticker.info.get("sharesOutstanding"),
        pe_ratio=ticker.info.get("trailingPE"),  # Trailing P/E Ratio = Current Market Price per Share / Earnings per Share (EPS) over the past 12 months
        eps=ticker.info.get("trailingEps"),  # Earnings per share = (Net Income − Dividends on Preferred Stock) / Average Outstanding Share
    )

    return {
        "price": day_stock_price.model_dump(), "other_data": extract_yticker_as_json(ticker)
    }


async def execute_job(db_connection, job_id: str, country_code: Literal["USA", "IND"]):
    timezone, calendar = None, None
    if country_code == "USA":
        timezone = USA_TIMEZONE
        calendar = xcals.get_calendar("XNYS")  # New York Stock Exchange
    elif country_code == "IND":
        timezone = IND_TIMEZONE
        calendar = xcals.get_calendar("XBOM")  # Bombay stock exchange
    else:
        raise Exception(f"Unknown country: {country_code}")

    exchanges = await db_connection.fetch("SELECT * FROM exchange WHERE country_code = $1", country_code)
    exchanges = [dict(each) for each in exchanges]

    # Get today's date as per the exchange's timezone
    current_date = date(2024, 4, 5) # datetime.now(timezone).date()

    # Check if market is open or not
    is_market_open = calendar.is_session(current_date)

    # Add an entry to the calendar
    await db_connection.executemany('''
        INSERT INTO calendar (date, exchange_id, last_job_id, open, created_on, updated_on)
        VALUES ($1, $2, $3, $4, NOW(), NOW())
        ON CONFLICT (date, exchange_id)
        DO UPDATE SET
            open = EXCLUDED.open,
            updated_on = EXCLUDED.updated_on
        ''', [
        (
            current_date,
            row['id'],
            job_id,
            is_market_open
        ) for row in exchanges
    ])

    if not is_market_open:
        logging.info("Market is closed, exiting...")
        await db_connection.execute("""
           UPDATE job SET stage = $2, finished_status = $3, log = $4, updated_on = NOW() WHERE id = $1
       """, job_id, 'completed', 'success', "Market is closed")
        logging.info("Successfully updated job table with status")
        return

    logging.info("Fetching all the tickers for the exchanges...")
    query_input = ", ".join([f"'{row['id']}'" for row in exchanges])
    listings = await db_connection.fetch(f"""
        SELECT exchange.ticker as exchange_ticker,  exchange_listing.* FROM exchange_listing 
        LEFT JOIN exchange
        ON exchange.id = exchange_listing.exchange_id
        WHERE exchange_listing.exchange_id IN ({query_input}) LIMIT 20
    """)
    listings = [dict(each) for each in listings]
    logging.info("Successfully fetched the listings")

    import requests
    from requests.adapters import HTTPAdapter
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    async def _run_sync_in_async(executor, func, *args):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(executor, func, *args)
        return result
    executor = ThreadPoolExecutor(max_workers=1)
    tasks = [_run_sync_in_async(executor, scrape_ticker_for_date, listing['exchange_ticker'], listing['ticker'], current_date, session, None) for listing in listings]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    success, failed = 0, 0
    for index, result in enumerate(results):
        if isinstance(result, Exception):
            print("Failed", listings[index]['ticker'], get_stack_trace(result))
            failed += 1
        else:
            print("Success", listings[index]['ticker'], result['price'])
            success += 1
        print("------------------------")
    print(success, failed)

    logging.info("Preparing job log message")
    job_log_message = "In development"

    logging.info("Updating job table with success...")
    await db_connection.execute("""
       UPDATE job SET stage = $2, finished_status = $3, log = $4, updated_on = NOW() WHERE id = $1
   """, job_id, 'completed', 'success', job_log_message)
    logging.info("Successfully updated job table with status")


async def sync_stock_data(country_code: Literal["USA", "IND"]):
    async with Config.pool.acquire() as db_connection:
        logging.info(f"Starting a new job...")
        data = await db_connection.fetchrow('''
                INSERT INTO job (name, created_on, updated_on)
                VALUES ($1, NOW(), NOW())
                RETURNING *
            ''', 'stock_data_sync')
        job_id = data['id']
        logging.info(f"Successfully created a new job with id: {job_id}")

        try:
            await execute_job(db_connection, job_id, country_code)
        except Exception as e:
            logging.info("Updating job table with failure...")
            await db_connection.execute("""
                    UPDATE job SET stage = $2, finished_status = $3, log = $4, updated_on = NOW() WHERE id = $1
                """, job_id, 'completed', 'failure', get_stack_trace(e))
            logging.info("Successfully updated job table with status")
            raise e

if __name__ == "__main__":
    import asyncio

    async def _startup():
        try:
            await Config.init()
            await sync_stock_data("USA")
            # x = scrape_ticker_for_date("NASDAQ", "AAPL", date(2024, 4, 5))
            # print(x)
        finally:
            await Config.cleanup()

    import time
    start_time = time.time()  # Record the start time
    asyncio.run(_startup())
    end_time = time.time()  # Record the end time
    time_taken = end_time - start_time  # Calculate the time taken
    logging.info(f"Time taken: {time_taken} seconds")
