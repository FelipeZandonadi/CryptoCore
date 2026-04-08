import pytest
from data_ingestion.extract.reddit import RedditAuth
from requests.auth import HTTPBasicAuth


@pytest.fixture
def auth_service():
    return RedditAuth(
        client_id="mock_client_id",
        client_secret="mock_client_secret",
        username="mock_username",
        password="mock_password",
        user_agent="mock_user_agent",
    )

@pytest.fixture
def mock_requests_post(mocker):
    mock_response = mocker.Mock()
    mock_post = mocker.patch('data_ingestion.extract.reddit.requests.post', return_value=mock_response)

    return mock_post, mock_response

def test_reddit_auth_success(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "mock_token_123", "expires_in": 3600}

    token = auth_service.access_token()
    assert token == "mock_token_123"
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",
        },
        headers={"User-Agent": "mock_user_agent"},
    )


def test_reddit_auth_failure(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 401
    mock_response.json.return_value = {"error": "invalid_grant"}

    with pytest.raises(Exception) as exc_info:
        auth_service.access_token()
    assert "[401] Failed to obtain access token from Reddit API." in str(exc_info.value)
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",    },
        headers={"User-Agent": "mock_user_agent"},
    )

def test_reddit_auth_no_token(auth_service, mock_requests_post):
    mock_post, mock_response = mock_requests_post
    mock_response.status_code = 200
    mock_response.json.return_value = {"error": "invalid_grant"}

    with pytest.raises(Exception) as exc_info:
        auth_service.access_token()
    assert "Access token not found in Reddit API response." in str(exc_info.value)
    auth = HTTPBasicAuth("mock_client_id", "mock_client_secret")
    mock_post.assert_called_once_with(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data={
            "grant_type": "password",
            "username": "mock_username",
            "password": "mock_password",
        },
        headers={"User-Agent": "mock_user_agent"},
    )