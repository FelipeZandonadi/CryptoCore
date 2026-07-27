import pytest
from unittest.mock import MagicMock, patch
from data_ingestion.ingestors.reddit import RedditIngestor
from data_ingestion.load.s3_key import RedditS3Key


def _hive_key(subreddit='Bitcoin', head='t3_abc123', tail='t3_xyz789'):
    return (
        f'raw/reddit/subreddit={subreddit}/year=2026/month=04/day=15/'
        f'h-{head}-t-{tail}-tm-123456789.0.json'
    )


def _drill_to_day(mock_storage, subreddit='Bitcoin'):
    """Make latest_common_prefix walk year -> month -> day, returning the day prefix."""
    day_prefix = f'raw/reddit/subreddit={subreddit}/year=2026/month=04/day=15/'
    mock_storage.latest_common_prefix.side_effect = [
        f'raw/reddit/subreddit={subreddit}/year=2026/',
        f'raw/reddit/subreddit={subreddit}/year=2026/month=04/',
        day_prefix,
    ]
    return day_prefix


@pytest.fixture
def mock_extractor():
    return MagicMock()


@pytest.fixture
def mock_storage():
    return MagicMock()


@pytest.fixture
def ingestor(mock_extractor, mock_storage):
    return RedditIngestor(
        extractor=mock_extractor,
        storage=mock_storage,
        subreddits=['Bitcoin', 'Ethereum'],
    )


# ============================================
# ---------- Tests for RedditIngestor ----------
# ============================================


def test_get_last_checkpoint_success(ingestor, mock_storage):
    """Should drill the date partitions and return the head from the latest key."""
    day_prefix = _drill_to_day(mock_storage)
    mock_storage.latest_key.return_value = _hive_key(head='t3_abc123')

    checkpoint = ingestor._get_last_checkpoint('Bitcoin')

    assert checkpoint == 't3_abc123'
    mock_storage.latest_key.assert_called_once_with(
        prefix=day_prefix, sort_key=RedditS3Key.sort_key
    )
    assert mock_storage.latest_common_prefix.call_count == 3


def test_get_last_checkpoint_none_when_no_partitions(ingestor, mock_storage):
    """Should return None (and not call latest_key) if no partition exists yet."""
    mock_storage.latest_common_prefix.return_value = None

    checkpoint = ingestor._get_last_checkpoint('Bitcoin')

    assert checkpoint is None
    mock_storage.latest_key.assert_not_called()


def test_get_last_checkpoint_none_when_day_empty(ingestor, mock_storage):
    """Should return None if the latest day partition has no objects."""
    _drill_to_day(mock_storage)
    mock_storage.latest_key.return_value = None

    checkpoint = ingestor._get_last_checkpoint('Bitcoin')

    assert checkpoint is None


def test_get_last_checkpoint_no_match(ingestor, mock_storage):
    """Should return None if the latest key doesn't match the expected pattern."""
    _drill_to_day(mock_storage)
    mock_storage.latest_key.return_value = 'raw/reddit/subreddit=Bitcoin/weird.json'

    checkpoint = ingestor._get_last_checkpoint('Bitcoin')

    assert checkpoint is None


def test_ingest_subreddit_success(ingestor, mock_extractor, mock_storage):
    """Should fetch data and upload to S3 when threads are found."""
    # Setup
    subreddit = 'Bitcoin'
    last_checkpoint = 't3_old'
    _drill_to_day(mock_storage, subreddit)
    mock_storage.latest_key.return_value = _hive_key(
        subreddit=subreddit, head=last_checkpoint, tail='t3_tail'
    )

    mock_data = [
        {'data': {'children': [{'data': {'name': 't3_new_head'}}]}},
        {'data': {'children': [{'data': {'name': 't3_new_tail'}}]}},
    ]
    mock_extractor.batch.return_value = mock_data

    # Execute
    ingestor.ingest_subreddit(subreddit)

    # Assert
    mock_extractor.batch.assert_called_once_with(
        subreddit=subreddit, fullname=last_checkpoint, limit=25
    )

    # Verify upload was called. We use ANY for the key because it contains a timestamp
    mock_storage.upload.assert_called_once()
    args, kwargs = mock_storage.upload.call_args
    s3_key = kwargs.get('s3_key') or args[0]
    uploaded_data = kwargs.get('data') or args[1]

    assert f'raw/reddit/subreddit={subreddit}/' in s3_key
    assert 'h-t3_new_head-t-t3_new_tail' in s3_key
    assert uploaded_data == mock_data


def test_ingest_subreddit_no_data(ingestor, mock_extractor, mock_storage):
    """Should not upload anything if no data is fetched from Reddit."""
    mock_storage.latest_key.return_value = None
    mock_extractor.batch.return_value = []

    ingestor.ingest_subreddit('Bitcoin')

    mock_storage.upload.assert_not_called()


def test_run_orchestration(ingestor):
    """Should call ingest_subreddit for each subreddit configured at init."""
    with patch.object(ingestor, 'ingest_subreddit') as mock_ingest:
        ingestor.run()

        assert mock_ingest.call_count == 2
        mock_ingest.assert_any_call('Bitcoin')
        mock_ingest.assert_any_call('Ethereum')
