import re
from datetime import datetime
from data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)


class RedditIngestor:
    def __init__(self, extractor, storage):
        """
        Initializes the RedditIngestor.

        Args:
            extractor: An instance of RedditExtractor.
            storage: An instance of AWSServiceS3.
        """
        self.extractor = extractor
        self.storage = storage

    def _get_last_checkpoint(self, subreddit: str) -> str | None:
        """
        Retrieves the last processed 'head' fullname from the latest S3 object key.
        """
        prefix = f'raw/reddit/{subreddit}/'
        latest_key = self.storage.latest_key(prefix=prefix)

        if not latest_key:
            return None

        # Regex to extract the head (h-...) and tail (t-...) from the filename
        match = re.search(r'h-([^-]+)-t-([^-]+)-', latest_key)
        if match:
            head = match.group(1)
            logger.info(f'Last checkpoint found for {subreddit}: {head}')
            return head

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

        datestr = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().timestamp()

        s3_key = (
            f'raw/reddit/{subreddit}/{datestr}/'
            f'h-{head}-t-{tail}-tm-{timestamp}.json'
        )

        self.storage.upload(s3_key=s3_key, data=result)
        logger.info(f'Successfully ingested {subreddit} -> {s3_key}')

    def run(self, subreddits: list[str]) -> None:
        """
        Runs the ingestion process for a list of subreddits.
        """
        for subreddit in subreddits:
            try:
                self.ingest_subreddit(subreddit)
            except Exception as e:
                logger.error(f'Error during ingestion of {subreddit}: {e}')
