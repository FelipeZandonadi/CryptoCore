import json
import pytest
from moto import mock_aws

from data_ingestion.load.aws_s3 import AWSClientS3, AWSServiceS3


# =========================
# === AWSClientS3 Tests ===
# =========================


@pytest.fixture
def aws_mock():
    with mock_aws():
        yield


@pytest.fixture
def aws_s3_client(aws_mock):
    s3_client = AWSClientS3(
        aws_access_key_id='test1',
        aws_secret_access_key='test2',
        region_name='us-east-1',
    )
    yield s3_client


def test_aws_client_s3_success(aws_s3_client):
    client = aws_s3_client.client
    assert client is not None
    assert aws_s3_client.aws_access_key_id == 'test1'
    assert aws_s3_client.aws_secret_access_key == 'test2'
    assert aws_s3_client.region_name == 'us-east-1'


def test_aws_client_s3_failure(mocker):
    mocker.patch('boto3.client', side_effect=Exception('Failed to create S3 client'))

    with pytest.raises(Exception) as exc_info:
        AWSClientS3(
            aws_access_key_id='test1',
            aws_secret_access_key='test2',
            region_name='us-east-1',
        )

    assert 'Failed to create S3 client: Failed to create S3 client' in str(
        exc_info.value
    )


# ==========================
# === AWSServiceS3 Tests ===
# ==========================


@pytest.fixture
def aws_s3_service(aws_mock):
    client = AWSClientS3(
        aws_access_key_id='test1',
        aws_secret_access_key='test2',
        region_name='us-east-1',
    ).client
    bucket_name = 'test-bucket-go-to-mars'
    client.create_bucket(Bucket=bucket_name)
    service = AWSServiceS3(client=client, bucket_name=bucket_name)
    yield service


# upload tests
def test_aws_service_s3_upload_success(aws_s3_service):
    service = aws_s3_service
    mock_data = {'cripto': 'bitcoin', 'price': 676767.67}
    s3_key = 'go_to_mars/2037-01-01/mars.json'

    result = service.upload(s3_key, mock_data)

    assert result is True

    response = service.client.get_object(Bucket=service.bucket_name, Key=s3_key)

    conteudo_salvo = json.loads(response['Body'].read().decode('utf-8'))

    assert conteudo_salvo == mock_data


def test_aws_service_s3_upload_failure(aws_s3_service, mocker):
    service = aws_s3_service
    mock_data = {'cripto': 'bitcoin', 'price': 676767.67}
    s3_key = 'go_to_mars/2037-01-01/mars.json'

    mocker.patch.object(
        service.client, 'put_object', side_effect=Exception('Upload failed')
    )

    with pytest.raises(Exception) as exc_info:
        service.upload(s3_key, mock_data)

    assert 'Failed to upload data to S3: Upload failed' in str(exc_info.value)


# A simple lexicographic ranking over the key string, enough to exercise the
# generic service without coupling the test to any source-specific format.
def _lexicographic(key: str) -> tuple:
    return (key,)


def test_aws_service_s3_latest_key_success_1(aws_s3_service):
    service = aws_s3_service
    s3_key = 'go_to_mars/2037-01-01/mars.json'

    service.client.put_object(Bucket=service.bucket_name, Key=s3_key, Body='test')
    latest_key = service.latest_key('go_to_mars/', sort_key=_lexicographic)

    assert latest_key == s3_key


def test_aws_service_s3_latest_key_success_2(aws_s3_service):
    service = aws_s3_service
    s3_key1 = 'go_to_mars/2037-01-01/mars.json'
    s3_key2 = 'go_to_mars/2037-01-02/mars.json'

    service.client.put_object(Bucket=service.bucket_name, Key=s3_key1, Body='test1')
    service.client.put_object(Bucket=service.bucket_name, Key=s3_key2, Body='test2')
    latest_key = service.latest_key('go_to_mars/', sort_key=_lexicographic)

    assert latest_key == s3_key2


def test_aws_service_s3_latest_key_empty_bucket_returns_none(aws_s3_service):
    service = aws_s3_service

    latest_key = service.latest_key('go_to_mars/', sort_key=_lexicographic)

    assert latest_key is None


def test_aws_service_s3_latest_key_uses_injected_sort_key(aws_s3_service):
    """latest_key must rank by the caller-supplied sort_key, not by key order."""
    service = aws_s3_service
    key_a = 'go_to_mars/a.json'
    key_z = 'go_to_mars/z.json'

    service.client.put_object(Bucket=service.bucket_name, Key=key_a, Body='x')
    service.client.put_object(Bucket=service.bucket_name, Key=key_z, Body='x')

    # Inverted ranking: 'a' should now be considered the greatest.
    latest_key = service.latest_key(
        'go_to_mars/', sort_key=lambda k: (-ord(k.split('/')[-1][0]),)
    )

    assert latest_key == key_a


def test_aws_service_s3_latest_key_failure(aws_s3_service, mocker):
    service = aws_s3_service

    mocker.patch.object(
        service.client,
        'get_paginator',
        side_effect=Exception('Failed to list objects'),
    )

    with pytest.raises(Exception) as exc_info:
        service.latest_key('go_to_mars/', sort_key=_lexicographic)

    assert 'Failed to list objects in S3: Failed to list objects' in str(exc_info.value)


# latest_common_prefix tests


def test_latest_common_prefix_returns_greatest(aws_s3_service):
    service = aws_s3_service
    base = 'raw/reddit/subreddit=Bitcoin/'
    service.client.put_object(
        Bucket=service.bucket_name, Key=f'{base}year=2025/x.json', Body='x'
    )
    service.client.put_object(
        Bucket=service.bucket_name, Key=f'{base}year=2026/x.json', Body='x'
    )

    assert service.latest_common_prefix(base) == f'{base}year=2026/'


def test_latest_common_prefix_empty_returns_none(aws_s3_service):
    service = aws_s3_service

    assert service.latest_common_prefix('raw/reddit/subreddit=Nope/') is None
