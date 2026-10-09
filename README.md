# kinesis-to-s3-lambda

Kinesis stream consumer Lambda that batches records into S3 (JSON Lines).

```
lambda/    handler, Dockerfile, requirements.txt (deployed)
tests/     unit tests
events/    sample Kinesis events 
```

Env vars: `BUCKET_NAME` (required), `S3_PREFIX` (default `kinesis`).

Build: `docker build -t kinesis-to-s3 lambda/`
