import json
import boto3
from data_ingestion.utils.logger import get_logger
from datetime import datetime
from botocore.exceptions import ClientError

logger = get_logger(__name__)

def upload_json_to_s3(data: dict, bucket: str, s3_key: str) -> bool:
    '''
    Uploads a dictionary as a JSON object directly to an S3 bucket.

    Args:
        data (dict): The dictionary to upload.
        bucket (str): The name of your S3 bucket.
        before (str): The fullname of the previous batch.
        s3_key (str): The path/filename inside the bucket (e.g., 'raw/reddit/data.json').
    '''
    s3_client = boto3.client('s3')

    try:
        json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')

        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        logger.info(f'Successfully uploaded to s3://{bucket}/{s3_key}')
        return True
    except ClientError as e:
        logger.error(f'Failed to upload to S3: {e}')
        return False

def save_json(data: dict, file_path: str, pretty_print: bool = False) -> None:
    '''
    Serializes and saves a dictionary to a JSON file.

    This utility function handles the file writing process, allowing for either 
    a human-readable (indented) or a production-ready (compact) format. It 
    uses UTF-8 encoding and ensures non-ASCII characters are preserved.

    Args:
        data (dict): The dictionary object to be serialized.
        file_path (str): The absolute or relative destination path, including 
            the filename (e.g., 'data/output.json').
        pretty_print (bool, optional): If True, the JSON will be saved with 
            a 4-space indentation. If False, it will be saved in a compact 
            single-line format. Defaults to False.
    '''

    indent_value = 4 if pretty_print else None
    separators_value = None if pretty_print else (',', ':')

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(
            data, 
            f, 
            ensure_ascii=False, 
            indent=indent_value, 
            separators=separators_value
        )
