import json

import boto3

from data_ingestion.utils.logger import get_logger


logger = get_logger(__name__)


class AWSClientS3:
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str

    def __init__(
        self,
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
    ):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        try:
            self._client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
        except Exception as e:
            logger.error(f'Failed to create S3 client: {e}')
            raise Exception(f'Failed to create S3 client: {e}')

    @property
    def client(self):
        return self._client


class AWSServiceS3:
    def __init__(
        self,
        client,
        bucket_name: str,
    ):
        self.client = client
        self.bucket_name = bucket_name

    def upload(self, s3_key: str, data: dict) -> bool:
        try:
            json_data = json.dumps(data, ensure_ascii=False).encode('utf-8')
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
            )
            logger.info('Successfully uploaded to s3')
            logger.debug(f'uploaded in this s3 key: s3://{self.bucket_name}/{s3_key}')
            return True
        except Exception as e:
            logger.error(f'Failed to upload data to S3: {e}')
            raise Exception(f'Failed to upload data to S3: {e}')

    def latest_key(self, prefix: str) -> str | None:
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )

            latest_object = None
            for page in pages:
                contents = page.get('Contents', [])
                if contents:
                    page_latest = max(
                        contents,
                        key=lambda item: (item['LastModified'], item['Key']),
                    )
                    if (
                        latest_object is None
                        or page_latest['LastModified'] > latest_object['LastModified']
                    ):
                        latest_object = page_latest

            return latest_object['Key'] if latest_object else None

        except Exception as e:
            logger.error(f'Failed to list objects in S3: {e}')
            raise Exception(f'Failed to list objects in S3: {e}')
