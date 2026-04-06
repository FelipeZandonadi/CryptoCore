from data_ingestion.config.env_settings import AWSConfig, RedditConfig
from data_ingestion.utils.logger import get_logger
from data_ingestion.extract.api_extract import RedditExtractor
from data_ingestion.load.data_load import save_json, upload_json_to_s3
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import re

logger = get_logger(__name__)


def config_env() -> dict[str, dict[str, str]]:
    reddit_config: RedditConfig = RedditConfig()
    aws_config: AWSConfig = AWSConfig()

    return {
        "reddit": reddit_config.env,
        "aws": aws_config.env,
    }


def reddit_threads_extractor(
    extractor: RedditExtractor,
    bucket: str,
    subreddit: str,
    fullname: str = None,
) -> None:
    result: list[dict] = extractor.batch(
        subreddit=subreddit,
        fullname=fullname,
        limit=24,
    )

    # Se possui próxima thread em direção ao futuro, after é o tail
    # Se possui thread anterior em direção ao passado, before é o head
    if not result:
        logger.warning("No data fetched from Reddit API.")
        return

    head: str = (
        result[0]
        .get("data", {})
        .get("children", [{}])[0]
        .get("data", {})
        .get("name", "")
    )
    tail: str = (
        result[-1]
        .get("data", {})
        .get("children", [{}])[-1]
        .get("data", {})
        .get("name", "")
    )

    if not head or not tail:
        logger.error("Could not extract head or tail from the result.")
        raise Exception("Head or tail is missing in the Reddit API response.")

    datestr = datetime.now().strftime("%Y-%m-%d")

    s3_key = f"raw/reddit/{subreddit}/{datestr}/h-{head}-t-{tail}-tm-{datetime.now().timestamp()}.json"

    logger.info(f"Previous batch fullname: {head}")
    logger.info(f"Next batch fullname: {tail}")

    upload_json_to_s3(
        data=result,
        bucket=bucket,
        s3_key=s3_key,
    )


def get_lastest_file_name(bucket_name: str, subreddit: str):
    s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")

    try:
        paginas = paginator.paginate(
            Bucket=bucket_name, Prefix=f"raw/reddit/{subreddit}/"
        )

        bucket_vazio = True
        obj_mais_recente = None

        for pagina in paginas:
            if "Contents" in pagina:
                bucket_vazio = False

                objects = sorted(
                    pagina["Contents"], key=lambda x: x["LastModified"], reverse=True
                )
                if obj_mais_recente is None:
                    obj_mais_recente = objects[0]
                else:
                    if objects[0]["LastModified"] > obj_mais_recente["LastModified"]:
                        obj_mais_recente = objects[0]

        if bucket_vazio:
            return None
        else:
            return obj_mais_recente.get("Key")

    except ClientError as e:
        logger.error(f"Error accessing the bucket: {e.response['Error']['Message']}")


def runner():
    configs = config_env()
    logger.info("Configurations loaded successfully.")

    extractor: RedditExtractor = RedditExtractor(
        client_id=configs["reddit"]["client_id"],
        client_secret=configs["reddit"]["client_secret"],
        username=configs["reddit"]["username"],
        password=configs["reddit"]["password_account"],
        user_agent=configs["reddit"]["user_agent"],
    )

    subreddits: list[str] = [
        "CryptoCurrency",
        "Bitcoin",
        "Ethereum",
        "ethtrader",
        "dogecoin",
        "CryptoMoonshots",
        "btc",
        "BitcoinBeginners",
        "CryptoTechnology",
    ]

    for subreddit in subreddits:
        logger.info(f"Starting data extraction for subreddit: {subreddit}")
        bucket_name = configs["aws"]["s3_bucket_name"]

        last_obj_fullname = get_lastest_file_name(
            bucket_name=bucket_name, subreddit=subreddit
        )

        head: str = None

        if last_obj_fullname:
            matchs = re.search(r"h-([^-]+)-t-([^-]+)-", last_obj_fullname)
            logger.info(
                f"Last batch found for subreddit {subreddit}: head={matchs.group(1)}, tail={matchs.group(2)}"
            )
            head = matchs.group(1)

        reddit_threads_extractor(extractor, bucket_name, subreddit, fullname=head)
        logger.info(f"Finished data extraction for subreddit: {subreddit}")


if __name__ == "__main__":
    runner()
