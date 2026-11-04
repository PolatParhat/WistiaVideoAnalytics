"""S3 storage, DynamoDB watermarks and the Secrets Manager token.

Each class has the same methods as its local twin (LocalStorage, LocalWatermarkStore),
so the ingestion code does not know or care where it runs.
"""

import json
from datetime import date

import boto3
from botocore.exceptions import ClientError


class S3Storage:
    """Raw files in an S3 bucket. Writes are conditional, so an existing file is never overwritten."""

    def __init__(self, bucket, client=None):
        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def put_json(self, key, obj):
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(obj, indent=2).encode(),
                ContentType="application/json",
                IfNoneMatch="*",  # only create; fail if the key already exists (raw data is immutable)
            )
        except ClientError as error:
            if error.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise FileExistsError(f"refusing to overwrite s3://{self.bucket}/{key}") from error
            raise
        return f"s3://{self.bucket}/{key}"

    def uri(self, prefix=""):
        return f"s3://{self.bucket}/{prefix}"


class DynamoWatermarkStore:
    """One item per media: {media_id, last_loaded_date}. Strongly consistent reads, conditional writes."""

    def __init__(self, table_name, resource=None):
        self.table = (resource or boto3.resource("dynamodb")).Table(table_name)

    def get(self, media_id):
        item = self.table.get_item(Key={"media_id": media_id}, ConsistentRead=True).get("Item")
        return date.fromisoformat(item["last_loaded_date"]) if item else None

    def set(self, media_id, loaded_date):
        """Advance the watermark; never move it backwards. Returns False if it was already at/after loaded_date."""
        try:
            self.table.put_item(
                Item={"media_id": media_id, "last_loaded_date": loaded_date.isoformat()},
                # ISO dates sort as text, so "<" compares them correctly
                ConditionExpression="attribute_not_exists(media_id) OR last_loaded_date < :d",
                ExpressionAttributeValues={":d": loaded_date.isoformat()},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
        return True


def get_secret(secret_id, client=None):
    """The Wistia token from Secrets Manager. Never log the return value."""
    client = client or boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=secret_id)["SecretString"].strip()
