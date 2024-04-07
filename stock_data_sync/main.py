from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Optional, List, Literal
import httpx
import json
import exchange_calendars as xcals

from common.app_utils import USA_TIMEZONE, IND_TIMEZONE
from common.httpx_utils import httpx_raise_for_status, httpx_wrapper
from common.utils import csv_to_json, get_stack_trace
import logging

from stock_data_sync.config import Config


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

    # Get today's date
    now = datetime.now(timezone)

    # Check if market is open or not
    is_market_open = calendar.is_session(now.strftime("%Y-%m-%d"))

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
            now.date(),
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

    # TODO: Market open, start scraping

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
        finally:
            await Config.cleanup()

    import time
    start_time = time.time()  # Record the start time
    asyncio.run(_startup())
    end_time = time.time()  # Record the end time
    time_taken = end_time - start_time  # Calculate the time taken
    logging.info(f"Time taken: {time_taken} seconds")
