import re
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RedditS3Key:
    subreddit: str
    head: str
    tail: str
    year: str
    month: str
    day: str
    timestamp: float

    _PATTERN = re.compile(
        r'raw/reddit/subreddit=(?P<subreddit>[^/]+)/'
        r'year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/'
        r'h-(?P<head>[^-]+)-t-(?P<tail>[^-]+)-tm-(?P<timestamp>[\d.]+)\.json$'
    )
    _TEMPLATE = (
        'raw/reddit/subreddit={subreddit}/'
        'year={year}/month={month}/day={day}/'
        'h-{head}-t-{tail}-tm-{timestamp}.json'
    )

    # Hive-style date partitions to drill (in order) below the subreddit prefix.
    DATE_PARTITIONS = ('year', 'month', 'day')

    @staticmethod
    def subreddit_prefix(subreddit: str) -> str:
        """Base S3 prefix for a subreddit's partitions."""
        return f'raw/reddit/subreddit={subreddit}/'

    def to_s3_key(self) -> str:
        return self._TEMPLATE.format(
            subreddit=self.subreddit,
            year=self.year,
            month=self.month,
            day=self.day,
            head=self.head,
            tail=self.tail,
            timestamp=self.timestamp,
        )

    @classmethod
    def from_s3_key(cls, key: str) -> 'RedditS3Key':
        match = cls._PATTERN.search(key)
        if not match:
            raise ValueError(f'Cannot parse S3 key: {key}')
        return cls(
            subreddit=match.group('subreddit'),
            year=match.group('year'),
            month=match.group('month'),
            day=match.group('day'),
            head=match.group('head'),
            tail=match.group('tail'),
            timestamp=float(match.group('timestamp')),
        )

    @classmethod
    def sort_key(cls, key: str) -> tuple[float, str]:
        """
        Ranking function for `AWSServiceS3.latest_key`. Ranks by the `-tm-`
        timestamp embedded in the key; keys that don't match this source's
        format rank lowest so they never win.
        """
        match = cls._PATTERN.search(key)
        timestamp = float(match.group('timestamp')) if match else -1.0
        return (timestamp, key)

    @classmethod
    def build(cls, subreddit: str, head: str, tail: str) -> 'RedditS3Key':
        now = datetime.now()
        return cls(
            subreddit=subreddit,
            head=head,
            tail=tail,
            year=now.strftime('%Y'),
            month=now.strftime('%m'),
            day=now.strftime('%d'),
            timestamp=now.timestamp(),
        )
