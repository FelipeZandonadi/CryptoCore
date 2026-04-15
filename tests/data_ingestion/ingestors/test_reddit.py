import pytest
import re
from unittest.mock import MagicMock, patch
from datetime import datetime
from data_ingestion.ingestors.reddit import RedditIngestor

@pytest.fixture
def mock_extractor():
    return MagicMock()

@pytest.fixture
def mock_storage():
    return MagicMock()

@pytest.fixture
def ingestor(mock_extractor, mock_storage):
    return RedditIngestor(extractor=mock_extractor, storage=mock_storage)

# ============================================
# ---------- Tests for RedditIngestor ----------
# ============================================

def test_get_last_checkpoint_success(ingestor, mock_storage):
    """Should return the head fullname from the latest S3 key."""
    mock_storage.latest_key.return_value = "raw_hml/reddit/Bitcoin/2026-04-15/h-t3_abc123-t-t3_xyz789-tm-123456789.json"
    
    checkpoint = ingestor._get_last_checkpoint("Bitcoin")
    
    assert checkpoint == "t3_abc123"
    mock_storage.latest_key.assert_called_once_with(prefix="raw_hml/reddit/Bitcoin/")

def test_get_last_checkpoint_none(ingestor, mock_storage):
    """Should return None if no latest key is found in S3."""
    mock_storage.latest_key.return_value = None
    
    checkpoint = ingestor._get_last_checkpoint("Bitcoin")
    
    assert checkpoint is None

def test_get_last_checkpoint_no_match(ingestor, mock_storage):
    """Should return None if the latest key doesn't match the expected pattern."""
    mock_storage.latest_key.return_value = "raw_hml/reddit/Bitcoin/some-weird-file.json"
    
    checkpoint = ingestor._get_last_checkpoint("Bitcoin")
    
    assert checkpoint is None

def test_ingest_subreddit_success(ingestor, mock_extractor, mock_storage):
    """Should fetch data and upload to S3 when threads are found."""
    # Setup
    subreddit = "Bitcoin"
    last_checkpoint = "t3_old"
    mock_storage.latest_key.return_value = f"raw_hml/reddit/{subreddit}/2026-04-15/h-{last_checkpoint}-t-t3_tail-tm-123.json"
    
    mock_data = [
        {
            "data": {
                "children": [
                    {"data": {"name": "t3_new_head"}}
                ]
            }
        },
        {
            "data": {
                "children": [
                    {"data": {"name": "t3_new_tail"}}
                ]
            }
        }
    ]
    mock_extractor.batch.return_value = mock_data
    
    # Execute
    ingestor.ingest_subreddit(subreddit)
    
    # Assert
    mock_extractor.batch.assert_called_once_with(
        subreddit=subreddit,
        fullname=last_checkpoint,
        limit=25
    )
    
    # Verify upload was called. We use ANY for the key because it contains a timestamp
    mock_storage.upload.assert_called_once()
    args, kwargs = mock_storage.upload.call_args
    s3_key = kwargs.get('s3_key') or args[0]
    uploaded_data = kwargs.get('data') or args[1]
    
    assert f"raw_hml/reddit/{subreddit}/" in s3_key
    assert "h-t3_new_head-t-t3_new_tail" in s3_key
    assert uploaded_data == mock_data

def test_ingest_subreddit_no_data(ingestor, mock_extractor, mock_storage):
    """Should not upload anything if no data is fetched from Reddit."""
    mock_storage.latest_key.return_value = None
    mock_extractor.batch.return_value = []
    
    ingestor.ingest_subreddit("Bitcoin")
    
    mock_storage.upload.assert_not_called()

def test_run_orchestration(ingestor):
    """Should call ingest_subreddit for each subreddit in the list."""
    with patch.object(ingestor, 'ingest_subreddit') as mock_ingest:
        subreddits = ["Bitcoin", "Ethereum"]
        ingestor.run(subreddits)
        
        assert mock_ingest.call_count == 2
        mock_ingest.assert_any_call("Bitcoin")
        mock_ingest.assert_any_call("Ethereum")
