import os
from dotenv import load_dotenv
from data_ingestion.utils.logger import get_logger
from typing import Final

load_dotenv()

logger = get_logger(__name__)

class RedditConfig:
    client_id: str
    client_secret: str
    password_account: str
    user_agent: str
    username: str
    
    REQUIRED_KEYS: Final[list[str]] = [
        'REDDIT_CLIENT_ID',
        'REDDIT_CLIENT_SECRET',
        'REDDIT_PASSWORD_ACCOUNT',
        'REDDIT_USER_AGENT',
        'REDDIT_USERNAME',
    ]

    def __init__(self):
        self._load_config()
        logger.info('Reddit configuration loaded successfully.')

    def _load_config(self) -> None:
        '''
        Loads the environment variables and ensures they are not None.
        (Type Narrowing).
        '''
        
        for key in self.REQUIRED_KEYS:
            value = os.getenv(key)
            
            if value is None:
                logger.error(f'{key} is not set in environment variables.')
                raise ValueError(f'{key} is not set in environment variables.')
            
            attribute_name = key.lower().replace('reddit_', '')
            
            setattr(self, attribute_name, value)

    @property
    def env(self) -> dict[str, str]:

        return {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'password_account': self.password_account,
            'user_agent': self.user_agent,
            'username': self.username,
        }


class AWSConfig():
    
    access_key_id: str
    secret_access_key: str
    default_region: str
    bucket_name: str
    
    REQUIRED_KEYS: Final[list[str]] = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION',
        'S3_BUCKET_NAME',
    ]
    
    def __init__(self):
        self._load_config()
        logger.info('AWS configuration loaded successfully.')
        
    def _load_config(self) -> None:
        '''
        Loads the environment variables and ensures they are not None.
        (Type Narrowing).
        '''
        
        for key in self.REQUIRED_KEYS:
            value = os.getenv(key)
            
            if value is None:
                logger.error(f'{key} is not set in environment variables.')
                raise ValueError(f'{key} is not set in environment variables.')
            
            attribute_name = key.lower().replace('aws_', '')
            
            setattr(self, attribute_name, value)
        
    @property
    def env(self) -> dict[str, str]:
        return {
            'access_key_id': self.access_key_id,
            'secret_access_key': self.secret_access_key,
            'default_region': self.default_region,
            's3_bucket_name': self.s3_bucket_name,
        }