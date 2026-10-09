import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def lambda_handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    prefix = os.environ.get("S3_PREFIX", "kinesis")

    items = []
    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        items.append(json.loads(payload))

    if not items:
        return {"processed": 0}

    now = datetime.now(timezone.utc)
    key = f"{prefix}/{now:%Y/%m/%d/%H}/{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(items, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote %d records to s3://%s/%s", len(items), bucket, key)
    return {"processed": len(items), "key": key}
