from data_ingestion.utils.logger import get_logger
from data_ingestion.config.env_settings import AWSConfig
from data_ingestion.load.aws_s3 import AWSClientS3, AWSServiceS3
from data_ingestion.ingestors.base import BaseIngestor
from data_ingestion.ingestors.reddit import build_reddit_ingestor
from time import perf_counter as pc


logger = get_logger(__name__)


def runner():
    clock = {'init': pc(), 'end': 0}

    logger.info('CryptoCore ingestion data_ingestion start')

    # 1. Storage (compartilhado entre todas as fontes)
    aws_config = AWSConfig()
    aws_client = AWSClientS3(
        aws_access_key_id=aws_config.access_key_id,
        aws_secret_access_key=aws_config.secret_access_key,
        region_name=aws_config.default_region,
    )
    aws_service = AWSServiceS3(
        client=aws_client.client,
        bucket_name=aws_config.bucket_name,
    )

    # 2. Ingestors
    ingestors: list[BaseIngestor] = [
        build_reddit_ingestor(aws_service),
        # build_binance_ingestor(aws_service),
    ]

    # 3. Run
    for ingestor in ingestors:
        ingestor.run()

    clock['end'] = pc()

    logger.info(
        f'CryptoCore data_ingestion finished in {(clock.get("end") - clock.get("init")):.2f} seconds'
    )
