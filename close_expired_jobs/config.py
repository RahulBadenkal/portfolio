import os

from dotenv import load_dotenv

from common.base_config import BaseConfig


class Config(BaseConfig):
    expired_time: int

    @staticmethod
    async def init():
        project_dir_path = os.path.dirname(__file__)
        load_dotenv(os.path.join(project_dir_path, '../.env_common'))  # Common dotenv
        load_dotenv(os.path.join(project_dir_path, '../.env_close_expired_job'))  # App specific dotenv

        # Call the base config init
        await super(Config, Config).init()

        # Set expired time
        BaseConfig.expired_time = int(os.getenv("EXPIRED_TIME", 15*60))




