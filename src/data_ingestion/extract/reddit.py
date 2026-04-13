import requests
from requests.auth import HTTPBasicAuth
from datetime import timedelta
from data_ingestion.utils.logger import get_logger

logger = get_logger(__name__)


class RedditAuth:
    """
    A class to handle Reddit API authentication using OAuth2.

    Attributes:
        client_id (str): The client ID for Reddit API.
        client_secret (str): The client secret for Reddit API.
        username (str): The Reddit username.
        password (str): The Reddit password.
        user_agent (str): The application name
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        username: str,
        password: str,
        user_agent: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.user_agent = user_agent

    def access_token(self) -> str:
        """
        Build access token for Reddit API using OAuth2.

        Returns:
            str: The access token.
        """
        auth: HTTPBasicAuth = HTTPBasicAuth(self.client_id, self.client_secret)
        data: dict[str, str] = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

        try:
            response = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=auth,
                data=data,
                headers={"User-Agent": self.user_agent},
            )
        except requests.RequestException as e:
            logger.error(f"Error obtaining access token: {e}")
            raise Exception(f"Error obtaining access token: {e}")
        
        token = response.json().get("access_token")

        if response.status_code != 200:
            logger.error(f"Failed to obtain access token {response.status_code}: {response.text}")
            raise Exception(f"[{response.status_code}] Failed to obtain access token from Reddit API.")
        if token is None:
            logger.error(f"Access token not found in response: {response.text}")
            raise Exception("Access token not found in Reddit API response.")

        logger.info(
            f"Access token obtained successfully. Expires in {timedelta(seconds=response.json().get('expires_in', 0))}."
        )
        return token


class RedditExtractor:
    """
    A class to extract data raw (the format is json) from Subreddit using the requests library.

    Attributes:
        client_id (str): The client ID for Reddit API.
        client_secret (str): The client secret for Reddit API.
        username (str): The Reddit username.
        password (str): The Reddit password.
        user_agent (str): The application name
    """

    base_url: str
    token: str
    user_agent: str
    headers: dict[str, str]

    def __init__(
        self,
        token: str,
        user_agent: str,
    ):
        self.base_url = "https://oauth.reddit.com"
        self.token = token
        self.user_agent = user_agent

        self.headers = {
            "Authorization": f"bearer {self.token}",
            "User-Agent": self.user_agent,
        }
        logger.info(f"RedditExtractor initialized")

    def fetch_thread_before(self, subreddit: str, fullname: str, limit: int = 25) -> dict:
        thread_endpoint = f"/r/{subreddit}/new"
        url = f"{self.base_url}{thread_endpoint}"
        params = {
            "limit": limit,
            "before": fullname,
            }
        
        response = requests.get(url, headers=self.headers, params=params)

        if response.status_code == 200:
            logger.info(f"Fetched thread successfully from subreddit: {subreddit}")
            return response.json()
        else:
            logger.error(f"Failed to fetch thread from subreddit: {subreddit}")
            raise Exception(f"[{response.status_code}] Failed to fetch thread from subreddit: {subreddit}")

    def batch(
        self,
        subreddit: str,
        fullname: str,
        limit: int = 25,
    ) -> list[dict]:
        """
        Performs an incremental sync of new threads using a pagination anchor.

        This method traverses the subreddit feed backwards from a specific point
        (the 'fullname' anchor) towards the most recent post. It uses the 'before'
        parameter to fetch batches of data until no newer items are found.

        Args:
            subreddit (str): The name of the subreddit to synchronize.
            fullname (str): The fullname (type_id) of the item to use as the
                anchor point for the slice. The sync fetches items created
                after this point.
            limit (int, optional): The maximum number of items to return in
                each slice of the listing. Defaults to 25 (max is 100).

        Returns:
            list[dict]: A list of JSON response dictionaries containing the
                newly fetched batches of threads.
        """

        result: list = []
        before: str = fullname

        while before is not None:
            try:
                response = self.fetch_thread_before(subreddit=subreddit, fullname=before, limit=limit)
            except Exception as e:
                logger.error(f"Error occurred while fetching threads from subreddit: {subreddit}")
                raise e

            if len(response.get("data", {}).get("children", [])) == 0:
                before = None
            else:
                logger.info(
                    f"Fetched threads successfully from subreddit: {subreddit}"
                )

                before = (
                    response.get("data", {})
                    .get("children", [{}])[0]
                    .get("data", {})
                    .get("name", "")
                )

                result.insert(0, response)

        return result

    def fetch_comments(self, subreddit) -> None:
        comments_endpoint = f"#"
        pass
