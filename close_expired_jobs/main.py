import asyncio
import logging

from close_expired_jobs.config import Config


async def execute_job(connection):
    # Fetch job entries that are not completed and expired
    expired_jobs = await connection.fetch(f"""
        SELECT id FROM job WHERE NOW() - created_on > INTERVAL '{Config.expired_time} seconds' AND stage != 'completed' LIMIT 1000
    """)

    # Extract job IDs
    job_ids = [job['id'] for job in expired_jobs]
    logging.info(f"Job ids to force close: {len(job_ids)}")

    if len(job_ids) > 0:
        async with connection.transaction():
            await connection.execute("""
                UPDATE job SET stage = 'completed', finished_status = 'failure', log = 'force closed expired jobs', updated_on = NOW() WHERE id = ANY($1)
            """, job_ids)
        logging.info(f"Successfully updated {len(job_ids)} job(s) to 'completed' with 'failure' status.")


async def close_jobs():
    async with Config.pool.acquire() as connection:
        await execute_job(connection)


if __name__ == "__main__":
    async def _startup():
        try:
            await Config.init()
            await close_jobs()
        finally:
            await Config.cleanup()

    import time
    start_time = time.time()  # Record the start time

    asyncio.run(_startup())

    end_time = time.time()  # Record the end time
    time_taken = end_time - start_time  # Calculate the time taken
    logging.info(f"Time taken: {time_taken} seconds")
