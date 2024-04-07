import asyncio
import csv
import io
import logging
import traceback
from datetime import datetime, date


def get_stack_trace(exception):
    """
    This function takes an exception as input and returns its stack trace as a string.

    :param exception: The exception from which to extract the stack trace.
    :return: A string containing the formatted stack trace of the exception.
    """
    # Extract the type, value, and traceback from the exception
    exc_type, exc_value, exc_traceback = type(exception), exception, exception.__traceback__

    # Format the traceback
    stack_trace = traceback.format_exception(exc_type, exc_value, exc_traceback)

    # Join the traceback list into a single string and return
    return ''.join(stack_trace)


def batch_split(items, batch_size):
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


def find_in_array(array, func):
    for index, item in enumerate(array):
        if func(index, item):
            return item
    return None


async def retry_handler(func, name, attempts=2, delay=0, *args, **kwargs):
    for attempt in range(attempts):
        try:
            logging.info(f"Attempt {attempt+1}/{attempts} for {name}...")
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            msg = f"Attempt {attempt + 1}/{attempts} for {name} failed."
            if attempt + 1 >= attempts:
                logging.error(msg)
                raise
            logging.warning(msg + get_stack_trace(e))
            if delay > 0:
                await asyncio.sleep(delay)


def csv_to_json(data, auto_correct_null=True):
    csv_file_like_object = io.StringIO(data)
    reader = csv.DictReader(csv_file_like_object)
    data = list(reader)
    if auto_correct_null:
        for row in data:
            for key, value in row.items():
                if value == 'null':
                    row[key] = None
    return data


def json_serial(datetime_fmt: str = None):
    """JSON serializer for objects not serializable by default json code"""

    def _serialize(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    return _serialize