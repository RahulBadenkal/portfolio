from dataclasses import dataclass
from datetime import datetime
from functools import reduce
from typing import Optional, List, Literal
import httpx
import json
from common.httpx_utils import httpx_raise_for_status, httpx_wrapper
from common.utils import csv_to_json, get_stack_trace
import logging

from exchange_listing_sync.config import Config


@dataclass
class ExchangeListing:
    exchange: Literal["NYSE", "NASDAQ", "NSE"]
    country_code: Literal["USA", "IND"]
    ticker: str
    isin: Optional[str]
    name: str
    asset_type: Literal["EQUITY", "ETF", "MF"]
    listing_date: datetime
    de_listing_date: Optional[datetime]
    is_active: bool


async def get_us_listings(client: httpx.AsyncClient = None) -> List[ExchangeListing]:
    async def _api(_client: httpx.AsyncClient):
        response = await _client.get("https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo")
        httpx_raise_for_status(response)
        data = csv_to_json(response.text)
        with open('data.json', "w") as f:
            json.dump(data, f, indent=2)

        # with open("data.json", "r") as f:
        #     data = json.load(f)

        data = [row for row in data if row['exchange'] in ['NYSE', "NASDAQ"]]
        formatted_data = [
            ExchangeListing(
                exchange=row['exchange'],
                country_code="USA",
                ticker=row['symbol'],
                isin=None,
                name=row['name'],
                asset_type='EQUITY' if row['assetType'] == 'Stock' else row['assetType'].upper(),
                listing_date=datetime.strptime(row['ipoDate'], "%Y-%m-%d"),
                de_listing_date=datetime.strptime(row['delistingDate'], "%Y-%m-%d") if row[
                                                                                           'delistingDate'] is not None else None,
                is_active=row['status'] == "Active"
            )
            for row in data
        ]
        return formatted_data

    data = await httpx_wrapper(_api, client)
    return data


async def get_india_listings(client: httpx.AsyncClient = None) -> List[ExchangeListing]:
    async def _api(_client: httpx.AsyncClient):
        # helpful links
        # https://medium.com/@arunes007/extract-all-listed-companies-in-indian-stock-market-nse-bse-688b525f3d20
        # https://www.nseindia.com/market-data/securities-available-for-trading
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        response = await _client.get(url, headers=headers, timeout=10)
        httpx_raise_for_status(response)
        data = csv_to_json(response.text)

        data = [{header.strip(): value for header, value in row.items()} for row in data]
        data = [row for row in data if row['SERIES'] == 'EQ']
        formatted_data = [
            ExchangeListing(
                exchange="NSE",
                country_code="IND",
                ticker=row['SYMBOL'],
                isin=row['ISIN NUMBER'],
                name=row['NAME OF COMPANY'],
                asset_type='EQUITY',
                listing_date=datetime.strptime(row['DATE OF LISTING'], "%d-%b-%Y"),
                de_listing_date=None,
                is_active=True
            )
            for row in data
        ]
        return formatted_data

    data = await httpx_wrapper(_api, client)
    return data


async def execute_job(db_connection, job_id: str):
    async with httpx.AsyncClient() as client:
        logging.info("Fetching all exchanges...")
        exchanges = await db_connection.fetch('SELECT * FROM exchange')
        exchanges = [dict(each) for each in exchanges]
        logging.info(f"Fetched {len(exchanges)} exchanges")

        data = []
        logging.info("Fetching listings for USA")
        us_data = await get_us_listings(client)
        data.extend(us_data)
        logging.info(f'Got {len(us_data)} listings from USA')

        logging.info("Fetching listings for IND")
        india_data = await get_india_listings(client)
        data.extend(india_data)
        logging.info(f'Got {len(india_data)} listings from IND')

        logging.info(f'Total listings {len(data)}')

        if len(data) > 0:
            logging.info("Updating listings table...")
            exchange_id_map = {each['ticker']: each['id'] for each in exchanges}
            await db_connection.executemany('''
                    INSERT INTO exchange_listing (exchange_id, last_job_id, ticker, isin, name, asset_type, listing_date, de_listing_date, is_active, created_on, updated_on)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW())
                    ON CONFLICT (exchange_id, ticker)
                    DO UPDATE SET
                        last_job_id = EXCLUDED.last_job_id,
                        isin = EXCLUDED.isin,
                        name = EXCLUDED.name,
                        asset_type = EXCLUDED.asset_type,
                        listing_date = EXCLUDED.listing_date,
                        de_listing_date = EXCLUDED.de_listing_date,
                        is_active = EXCLUDED.is_active,
                        updated_on = EXCLUDED.updated_on
                ''', [
                (
                    exchange_id_map[row.exchange],
                    job_id,
                    row.ticker,
                    row.isin,
                    row.name,
                    row.asset_type,
                    row.listing_date,
                    row.de_listing_date,
                    row.is_active,
                ) for row in data
            ])

        logging.info("Preparing job log message")
        def _reducer(accumulator, item: ExchangeListing):
            # Create a key from 'country_code' and 'exchange'
            key = (item.country_code, item.exchange)
            # Increment the count for the key in the accumulator
            if key in accumulator:
                accumulator[key] += 1
            else:
                accumulator[key] = 1
            return accumulator

        grouped_counts = reduce(_reducer, data, {})
        job_log_message = "\n".join([f"{key}: {value}" for key, value in grouped_counts.items()])
        job_log_message += "\n" + f"Total: {len(data)}"
        job_log_message = job_log_message.strip()
        logging.info("Updating job table with success...")
        await db_connection.execute("""
                   UPDATE job SET stage = $2, finished_status = $3, log = $4, updated_on = NOW() WHERE id = $1
               """, job_id, 'completed', 'success', job_log_message)
        logging.info("Successfully updated job table with status")


async def sync_listings():
    async with Config.pool.acquire() as db_connection:
        logging.info(f"Starting a new job...")
        data = await db_connection.fetchrow('''
                INSERT INTO job (name, created_on, updated_on)
                VALUES ($1, NOW(), NOW())
                RETURNING *
            ''', 'exchange_listing_sync')
        job_id = data['id']
        logging.info(f"Successfully created a new job with id: {job_id}")

        try:
            await execute_job(db_connection, job_id)
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
            await sync_listings()
        finally:
            await Config.cleanup()


    import time

    start_time = time.time()  # Record the start time

    asyncio.run(_startup())

    end_time = time.time()  # Record the end time
    time_taken = end_time - start_time  # Calculate the time taken
    logging.info(f"Time taken: {time_taken} seconds")
