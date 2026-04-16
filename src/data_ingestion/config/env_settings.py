from pydantic_settings import BaseSettings, SettingsConfigDict
from data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)


class RedditConfig(BaseSettings):
    client_id: str
    client_secret: str
    password_account: str
    user_agent: str
    username: str

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_prefix='REDDIT_',
        extra='ignore',
    )

    @property
    def env(self) -> dict[str, str]:
        logger.info('Reddit configuration loaded successfully.')
        return self.model_dump()


class AWSConfig(BaseSettings):
    access_key_id: str
    secret_access_key: str
    default_region: str
    bucket_name: str

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_prefix='AWS_',
        extra='ignore',
    )

    @property
    def env(self) -> dict[str, str]:
        logger.info('AWS configuration loaded successfully.')
        return self.model_dump()
