import pytest
from data_ingestion.load.s3_key import RedditS3Key


VALID_KEY = (
    'raw/reddit/subreddit=Bitcoin/year=2026/month=05/day=03/'
    'h-t3_abc123-t-t3_xyz789-tm-1746230400.0.json'
)


# ============================================
# ---------- Tests for RedditS3Key -----------
# ============================================


def test_from_s3_key_parses_all_fields():
    key = RedditS3Key.from_s3_key(VALID_KEY)

    assert key.subreddit == 'Bitcoin'
    assert key.year == '2026'
    assert key.month == '05'
    assert key.day == '03'
    assert key.head == 't3_abc123'
    assert key.tail == 't3_xyz789'
    assert key.timestamp == 1746230400.0


def test_from_s3_key_raises_on_invalid_key():
    with pytest.raises(ValueError, match='Cannot parse S3 key'):
        RedditS3Key.from_s3_key('raw/reddit/subreddit=Bitcoin/some-weird-file.json')


def test_from_s3_key_raises_on_legacy_key():
    """Legacy (non-Hive) keys must not parse under the new format."""
    legacy = 'raw/reddit/Bitcoin/2026-05-03/h-t3_a-t-t3_b-tm-1746230400.0.json'
    with pytest.raises(ValueError, match='Cannot parse S3 key'):
        RedditS3Key.from_s3_key(legacy)


def test_to_s3_key_roundtrip():
    """Parsing and re-serializing a key should return the original string."""
    key_obj = RedditS3Key.from_s3_key(VALID_KEY)
    assert key_obj.to_s3_key() == VALID_KEY


def test_build_produces_valid_parseable_key():
    key_obj = RedditS3Key.build(subreddit='Ethereum', head='t3_head1', tail='t3_tail1')
    s3_key = key_obj.to_s3_key()

    parsed = RedditS3Key.from_s3_key(s3_key)
    assert parsed.subreddit == 'Ethereum'
    assert parsed.head == 't3_head1'
    assert parsed.tail == 't3_tail1'


def test_build_sets_current_date():
    from unittest.mock import patch
    from datetime import datetime

    fixed_dt = datetime(2026, 5, 3, 12, 0, 0)
    with patch('data_ingestion.load.s3_key.datetime') as mock_dt:
        mock_dt.now.return_value = fixed_dt
        key_obj = RedditS3Key.build('Bitcoin', 't3_h', 't3_t')

    assert key_obj.year == '2026'
    assert key_obj.month == '05'
    assert key_obj.day == '03'
    assert key_obj.timestamp == fixed_dt.timestamp()


def test_subreddit_prefix():
    assert RedditS3Key.subreddit_prefix('Bitcoin') == 'raw/reddit/subreddit=Bitcoin/'


# ---------- sort_key ----------


def test_sort_key_ranks_by_tm_timestamp():
    older = (
        'raw/reddit/subreddit=Bitcoin/year=2026/month=05/day=01/'
        'h-t3_a-t-t3_b-tm-1000.0.json'
    )
    newer = (
        'raw/reddit/subreddit=Bitcoin/year=2026/month=05/day=01/'
        'h-t3_c-t-t3_d-tm-2000.0.json'
    )
    assert RedditS3Key.sort_key(newer) > RedditS3Key.sort_key(older)


def test_sort_key_ranks_unparseable_keys_lowest():
    valid = RedditS3Key.sort_key(VALID_KEY)
    garbage = RedditS3Key.sort_key('raw/reddit/subreddit=Bitcoin/whatever.json')

    assert garbage < valid
    assert garbage[0] == -1.0
