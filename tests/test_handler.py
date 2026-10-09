import json
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from app import handler  # noqa: E402


def test_writes_records_to_s3(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "bucket")
    path = os.path.join(os.path.dirname(__file__), "..", "events", "kinesis_event.json")
    with open(path) as f:
        event = json.load(f)
    with patch.object(handler, "s3") as s3:
        result = handler.lambda_handler(event, None)
    assert result["processed"] == 1
    s3.put_object.assert_called_once()
