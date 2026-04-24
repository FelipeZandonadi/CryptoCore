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

    # 1. Config
    reddit_config = RedditConfig()
    aws_config = AWSConfig()

    # 2. Auth
    token: str = RedditAuth(
        client_id=reddit_config.client_id,
        client_secret=reddit_config.client_secret,
        username=reddit_config.username,
        password=reddit_config.password_account,
        user_agent=reddit_config.user_agent,
    ).access_token()

    # 3. Extractor
    red_extractor: RedditExtractor = RedditExtractor(
        token=token,
        user_agent=reddit_config.user_agent,
    )

    # 4. Storage
    aws_client: AWSClientS3 = AWSClientS3(
        aws_access_key_id=aws_config.access_key_id,
        aws_secret_access_key=aws_config.secret_access_key,
        region_name=aws_config.default_region,
    )

    aws_service: AWSServiceS3 = AWSServiceS3(
        client=aws_client.client,
        bucket_name=aws_config.bucket_name,
    )

    # 5. Ingestor
    red_ingestor: RedditIngestor = RedditIngestor(
        extractor=red_extractor,
        storage=aws_service,
    )

    # 6. Run
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

    red_ingestor.run(subreddits)

    clock['end'] = pc()

    logger.info(
        f'CryptoCore data_ingestion finished in {(clock.get("end") - clock.get("init")):.2f} seconds'
    )
