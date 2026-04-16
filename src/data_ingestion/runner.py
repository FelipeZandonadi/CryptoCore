from data_ingestion.utils.logger import get_logger
from data_ingestion.config.env_settings import AWSConfig, RedditConfig
from data_ingestion.extract.reddit import RedditExtractor, RedditAuth
from data_ingestion.load.aws_s3 import AWSClientS3, AWSServiceS3
from data_ingestion.ingestors.reddit import RedditIngestor
from time import perf_counter as pc


logger = get_logger(__name__)


def runner():
    clock = {'init': pc(), 'end': 0}

    logger.info('CryptoCore ingestion data_ingestion start')

    # ========
    # get envs
    # ========
    reddit_config: RedditConfig = RedditConfig()
    aws_config: AWSConfig = AWSConfig()

    red_env: dict[str, str] = reddit_config.env
    aws_env: dict[str, str] = aws_config.env

    # ===================
    # instance extractor
    # ===================
    token: str = RedditAuth(
        client_id=red_env['client_id'],
        client_secret=red_env['client_secret'],
        username=red_env['username'],
        password=red_env['password_account'],
        user_agent=red_env['user_agent'],
    ).access_token()

    red_extractor: RedditExtractor = RedditExtractor(
        token=token,
        user_agent=red_env['user_agent'],
    )

    # ======================
    # instance aws s3 loader
    # ======================
    aws_client: AWSClientS3 = AWSClientS3(
        aws_access_key_id=aws_env['access_key_id'],
        aws_secret_access_key=aws_env['secret_access_key'],
        region_name=aws_env['default_region'],
    )

    aws_service: AWSClientS3 = AWSServiceS3(
        client=aws_client.client,
        bucket_name=aws_env['bucket_name'],
    )

    # =================
    # instance ingestor
    # =================
    red_ingestor: RedditIngestor = RedditIngestor(
        extractor=red_extractor, storage=aws_service
    )

    # ========
    # pipeline
    # ========
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

    for subreddit in subreddits:
        red_ingestor.ingest_subreddit(subreddit=subreddit)

    clock['end'] = pc()

    logger.info(
        f'CryptoCore data_ingestion finished in {(clock.get("end") - clock.get("init")):.2f} seconds'
    )
