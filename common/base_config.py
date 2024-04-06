import os
import logging
import sys
import time
from datetime import tzinfo, datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Any

import asyncpg
import pytz
from asyncpg import Pool
from dateutil import parser


class BaseConfig:
    project_dir_path: str
    tz: tzinfo
    pool: Pool = None

    @staticmethod
    def _set_timezone():
        timezone = os.getenv("TIMEZONE", 'UTC')

        # Set the timezone for the application
        os.environ['TZ'] = timezone
        time.tzset()

        # Store the tz locally also
        BaseConfig.tz = pytz.timezone(timezone)

    @staticmethod
    def _set_logger():
        class ColorFormatter(logging.Formatter):
            COLORS = {
                logging.INFO: '\033[97m',  # White
                logging.DEBUG: '\033[97m',  # White
                logging.WARNING: '\033[93m',  # Yellow
                logging.ERROR: '\033[91m',  # Red
                logging.CRITICAL: '\033[95m',  # Magenta
                'RESET': '\033[0m',  # Reset
            }

            def format(self, record):
                color = self.COLORS.get(record.levelno, '')
                reset = self.COLORS['RESET']
                message = super().format(record)
                return f"{color}{message}{reset}" if color else message

        logging.root.setLevel(logging.INFO)

        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        console_formatter = ColorFormatter(log_format) if int(os.getenv("DEV")) == 1 else logging.Formatter(log_format)
        file_formatter = logging.Formatter(log_format)

        # Single Stream Handler for stderr, with color-coded messages
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(console_formatter)

        # File Handler for logging to a file
        log_dir_path = os.getenv('LOG_DIR_PATH')
        file_handler, error_file_handler = None, None
        if log_dir_path:
            if not os.path.exists(log_dir_path):
                os.makedirs(log_dir_path, exist_ok=True)
            log_file_path = os.path.join(log_dir_path, 'app.log')
            file_handler = TimedRotatingFileHandler(log_file_path, when='midnight', interval=1, backupCount=30)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(file_formatter)

            # File Handler for logging only errors to a file
            error_log_file_path = os.path.join(log_dir_path, 'app.error.log')
            error_file_handler = TimedRotatingFileHandler(error_log_file_path, when='midnight', interval=1, backupCount=30)
            error_file_handler.setLevel(logging.ERROR)
            error_file_handler.setFormatter(file_formatter)

        logging.root.addHandler(stderr_handler)
        if file_handler:
            logging.root.addHandler(file_handler)
        if error_file_handler:
            logging.root.addHandler(error_file_handler)

        logging.info("******** NEW RUN INSTANCE **************")
        logging.info('Successfully setup logger')

    @staticmethod
    async def _set_db():
        def timestamptz_enocder(v):
            """ Support different format for the timestampz column.
                As of now asynpg doesn't have the feature to store tzaware dates, so it converts all to utc before storing
            """
            if isinstance(v, (int, float)):
                return datetime.fromtimestamp(v, tz=BaseConfig.tz).isoformat()
            if isinstance(v, datetime):
                return v.astimezone(BaseConfig.tz).isoformat()
            if isinstance(v, str):
                return parser.parse(v).astimezone(BaseConfig.tz)
            raise ValueError

        async def transform(conn: asyncpg.Connection):
            await conn.set_type_codec(schema='pg_catalog', typename="timestamptz", encoder=timestamptz_enocder,
                                      decoder=timestamptz_enocder)

        logging.info("Creating a connection pool...")
        BaseConfig.pool = await asyncpg.create_pool(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DATABASE"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            init=transform,
            command_timeout=120  # Set the default timeout for all operations (in seconds)
        )
        logging.info("Connection pool successfully created")

    @staticmethod
    async def init():
        BaseConfig.project_dir_path = os.path.join(os.path.dirname(__file__), "../")

        BaseConfig._set_timezone()

        # Setup logger
        BaseConfig._set_logger()

        # Setup database
        await BaseConfig._set_db()

    @staticmethod
    async def cleanup():
        if BaseConfig.pool:
            logging.info("Closing connection pool...")
            await BaseConfig.pool.close()
            logging.info("Closed connection pool successfully")
