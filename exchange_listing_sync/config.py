import os

from dotenv import load_dotenv

from common.base_config import BaseConfig


class Config(BaseConfig):
    @staticmethod
    async def init():
        curr_dir_path = os.path.dirname(__file__)
        load_dotenv(os.path.join(curr_dir_path, '../.env_common'))  # Common dotenv
        load_dotenv(os.path.join(curr_dir_path, '../.env_exchange_listing_sync'))  # App specific dotenv

        # Call the base config init
        await super(Config, Config).init()




