# Cloud Day 1 — S3 / Object Storage

## Goal

Practice the basic S3 object storage workflow (bucket, object, prefix, upload, download) using a local S3-compatible server and the AWS CLI, starting from a clean folder with no dependencies on earlier projects.

## Why local RustFS instead of AWS S3

The AWS account available had exhausted its Free Tier, and S3 has separate cost items (storage, requests, data transfer), so a local S3-compatible server was used to keep the cost at zero. The S3 API and CLI commands are the same; against real AWS only the `--endpoint-url` override would be dropped and real credentials used.

## Stack

- RustFS: local S3-compatible object storage (Docker, named volume for persistence)
- AWS CLI 2.37.4 (official installer)
- Bash

## What I Practiced

- Restarted a stopped RustFS container and health-checked it (`/health` on port 9000)
- Installed the AWS CLI with the official installer
- Passed lab credentials through environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
- Pointed the CLI at the local endpoint with `--endpoint-url`
- Created a bucket (`s3 mb`), uploaded (`s3 cp`), listed buckets and objects (`s3 ls`)
- Downloaded the object and verified it with `diff` (empty output = identical)
- Used a prefix (`raw/orders.csv`) and saw that `raw/` is not a real folder, only part of the object key
- Inspected object metadata without downloading it (`s3api head-object`)

## S3 Mental Model

```text
Bucket
└── Prefix
    └── Object

s3://de-day01/raw/orders.csv
     |         |    |
   bucket    prefix object   (bucket + "raw/orders.csv" = object key)
```

```text
Local file --upload--> Bucket / Object --download--> Local file
```

## Things That Tripped Me Up

- Environment variables set with `export` only live in that terminal window; a new terminal needs them set again.
- The AWS CLI does not read a `.env` file; that is a Python `python-dotenv` behavior.
- `cat > data/orders.csv` creates the file but not the `data/` folder; the folder has to exist first.
- A `\` at the end of a line continues the command on the next line; it must not appear in the middle of a one-line command.
- `aws s3` (high level, `cp`/`ls`/`mb`) and `aws s3api` (one command per API call, explicit `--bucket` / `--key`) are two different command families.
- The container had exited after a machine restart, but the bucket and its data were still there because of the named volume.

## Honest Status

This day was a guided walkthrough: most commands were shown first and then run. Only the final download (`s3 cp` from the bucket to a new local file) was written by me, with a hint. The real check is a blank-page repeat a few days later: new bucket, upload, list, download, `diff`, no hints.
