from data_ingestion.config.env_settings import AWSConfig, RedditConfig
from data_ingestion.utils.logger import get_logger
from data_ingestion.extract.api_extract import RedditExtractor
from data_ingestion.load.data_load import save_json, upload_json_to_s3
from datetime import datetime
import sys

logger = get_logger(__name__)

def config_env() -> dict[str, dict[str, str]]:
    reddit_config: RedditConfig = RedditConfig()
    aws_config: AWSConfig = AWSConfig()
    
    return {
        'reddit': reddit_config.env,
        'aws': aws_config.env,
    }

def runner():
    configs = config_env()
    logger.info('Configurations loaded successfully.')

    test: RedditExtractor = RedditExtractor(
        client_id=configs['reddit']['client_id'],
        client_secret=configs['reddit']['client_secret'],
        username=configs['reddit']['username'],
        password=configs['reddit']['password_account'],
        user_agent=configs['reddit']['user_agent'],
    )
    
    subreddits: list[str] = [
        'CryptoCurrency',
        'Bitcoin',
        'Ethereum',
        'ethtrader',
        'dogecoin',
        'CryptoMoonshots',
        'btc',
        'BitcoinBeginners',
        'CryptoTechnology',
    ]
    
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        result: list[dict] = test.bootstrap(
            subreddit=subreddits[0],
            limit=25,
        )

        # Se possui próxima thread em direção ao futuro, after é o tail
        # Se possui thread anterior em direção ao passado, before é o head
        # Para o bootstrap, o 
        head: str = result[0].get('data', {}).get('children', [{}])[0].get('data', {}).get('name', '')
        tail: str = result[-1].get('data', {}).get('after', '')
        datestr = datetime.now().strftime('%Y-%m-%d')

        s3_key = f'raw/reddit/{subreddits[0]}/{datestr}/b-{head}-a-{tail}{datetime.now().timestamp()}.json'

        logger.info(f'Previous batch fullname: {head}')
        logger.info(f'Next batch fullname: {tail}')

        upload_json_to_s3(
            data=result, 
            bucket=configs['aws']['s3_bucket_name'],
            s3_key=s3_key,
        )
    
    # result: list[dict] = test.bootstrap(
    #     subreddit=subreddits[0],
    #     limit=25,
    # )
    # upload_json_to_s3(result, 'cryptocore-data')

    # logger.debug('TESTE')
    # data = test.sync_next_batch(
    #     subreddit=subreddits[0],
    #     fullname='t3_1qwihpf',
    #     limit=25,
    # )

    # upload_json_to_s3(data, 'cryptocore-data')


if __name__ == '__main__':
    runner()