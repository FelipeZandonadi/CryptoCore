from pydantic_settings import BaseSettings, SettingsConfigDict

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
