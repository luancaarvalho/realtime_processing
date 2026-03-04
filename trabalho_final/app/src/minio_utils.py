from __future__ import annotations

import io
import json
from typing import Any

import boto3
from botocore.exceptions import ClientError
import pyarrow.parquet as pq

from settings import SETTINGS


def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=SETTINGS.minio_endpoint,
        aws_access_key_id=SETTINGS.minio_access_key,
        aws_secret_access_key=SETTINGS.minio_secret_key,
        region_name="us-east-1",
    )


def ensure_bucket(client) -> None:
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if SETTINGS.minio_bucket not in existing:
        client.create_bucket(Bucket=SETTINGS.minio_bucket)


def upload_bytes(client, key: str, payload: bytes, content_type: str) -> None:
    client.put_object(
        Bucket=SETTINGS.minio_bucket,
        Key=key,
        Body=payload,
        ContentType=content_type,
    )


def upload_json(client, key: str, data: dict[str, Any]) -> None:
    upload_bytes(client, key, json.dumps(data, ensure_ascii=False).encode("utf-8"), "application/json")


def upload_parquet(client, key: str, arrow_table) -> None:
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer)
    upload_bytes(client, key, buffer.getvalue(), "application/octet-stream")


def get_json(client, key: str) -> dict[str, Any] | None:
    try:
        resp = client.get_object(Bucket=SETTINGS.minio_bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        return None
    except Exception:
        return None

    payload = resp["Body"].read().decode("utf-8")
    return json.loads(payload)
