from data_ingestion.load.s3_key import RedditS3Key
from data_ingestion.load.aws_s3 import AWSServiceS3
from data_ingestion.extract.base import BaseExtractor
from data_ingestion.extract.reddit import RedditAuth, RedditExtractor
from data_ingestion.config.env_settings import RedditConfig
from data_ingestion.ingestors.base import BaseIngestor
from data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)


class RedditIngestor(BaseIngestor):
    def __init__(
        self,
        extractor: BaseExtractor,
        storage: AWSServiceS3,
        subreddits: list[str],
    ):
        self.extractor = extractor
        self.storage = storage
        self.subreddits = subreddits

    def _get_last_checkpoint(self, subreddit: str) -> str | None:
        """
        Retrieves the last processed 'head' fullname from the latest S3 object key.
        """
        prefix = f'raw/reddit/{subreddit}/'
        latest_key = self.storage.latest_key(prefix=prefix)

        if not latest_key:
            return None

        try:
            head = RedditS3Key.from_s3_key(latest_key).head
            logger.info(f'Last checkpoint found for {subreddit}: {head}')
            return head
        except ValueError:
            logger.warning(f'Could not parse checkpoint from key: {latest_key}')
            return None

    def ingest_subreddit(self, subreddit: str) -> None:
        """
        Orchestrates the data extraction and storage for a specific subreddit.
        """
        logger.info(f'Starting ingestion for subreddit: {subreddit}')

        last_fullname = self._get_last_checkpoint(subreddit)

        result = self.extractor.batch(
            subreddit=subreddit, fullname=last_fullname or '', limit=25
        )

        if not result:
            logger.warning(f'No new data fetched for subreddit: {subreddit}')
            return

        try:
            head = (
                result[0]
                .get('data', {})
                .get('children', [{}])[0]
                .get('data', {})
                .get('name', '')
            )
            tail = (
                result[-1]
                .get('data', {})
                .get('children', [{}])[-1]
                .get('data', {})
                .get('name', '')
            )
        except (IndexError, KeyError) as e:
            logger.error(f'Failed to extract head/tail for {subreddit}: {e}')
            return

        s3_key = RedditS3Key.build(
            subreddit=subreddit, head=head, tail=tail
        ).to_s3_key()

        self.storage.upload(s3_key=s3_key, data=result)
        logger.info(f'Successfully ingested {subreddit} -> {s3_key}')

    def run(self) -> None:
        for subreddit in self.subreddits:
            try:
                self.ingest_subreddit(subreddit)
            except Exception as e:
                logger.error(f'Error during ingestion of {subreddit}: {e}')


def build_reddit_ingestor(storage: AWSServiceS3) -> RedditIngestor:
    config = RedditConfig()
    token = RedditAuth(
        client_id=config.client_id,
        client_secret=config.client_secret,
        username=config.username,
        password=config.password_account,
        user_agent=config.user_agent,
    ).access_token()
    extractor = RedditExtractor(token=token, user_agent=config.user_agent)
    subreddits = [
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
    return RedditIngestor(extractor=extractor, storage=storage, subreddits=subreddits)
