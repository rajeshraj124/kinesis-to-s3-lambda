#!/usr/bin/env python3
"""Produce demo JSON records to an AWS Kinesis Data Stream.

Usage:
  pip install boto3
  python produce_kinesis_demo.py --stream my-stream --count 100 --interval 0.2
  # LocalStack: add --endpoint-url http://localhost:4566
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

MAX_RETRIES = 5


def make_record():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user-{random.randint(1, 50)}",
        "event_type": random.choice(["click", "view", "purchase", "login"]),
        "amount": round(random.uniform(1, 500), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stream", required=True, help="Kinesis stream name")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--count", type=int, default=100, help="records to send (0 = forever)")
    p.add_argument("--interval", type=float, default=0.5, help="seconds between batches")
    p.add_argument("--batch-size", type=int, default=10, help="records per put_records call (max 500)")
    p.add_argument("--endpoint-url", help="custom endpoint, e.g. LocalStack")
    args = p.parse_args()

    client = boto3.client("kinesis", region_name=args.region, endpoint_url=args.endpoint_url)
    sent = 0
    while args.count == 0 or sent < args.count:
        n = args.batch_size if args.count == 0 else min(args.batch_size, args.count - sent)
        records = []
        for _ in range(n):
            rec = make_record()
            records.append({"Data": (json.dumps(rec) + "\n").encode(), "PartitionKey": rec["user_id"]})
        pending = records
        for attempt in range(1, MAX_RETRIES + 1):
            resp = client.put_records(StreamName=args.stream, Records=pending)
            if resp["FailedRecordCount"] == 0:
                pending = []
                break
            pending = [r for r, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            print(f"retry {attempt}: {len(pending)} failed records")
            time.sleep(0.2 * 2**attempt)
        failed = len(pending)
        sent += n - failed
        print(f"sent batch of {n} (failed: {failed}) total={sent}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
