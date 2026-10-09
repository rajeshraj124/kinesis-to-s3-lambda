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

    lines = []
    for record in event.get("Records", []):
        payload = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        lines.append(payload.strip())

    if not lines:
        return {"processed": 0}

    now = datetime.now(timezone.utc)
    key = f"{prefix}/{now:%Y/%m/%d/%H}/{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.jsonl"
    s3.put_object(Bucket=bucket, Key=key, Body="\n".join(lines).encode("utf-8"))
    logger.info("Wrote %d records to s3://%s/%s", len(lines), bucket, key)
    return {"processed": len(lines), "key": key}
