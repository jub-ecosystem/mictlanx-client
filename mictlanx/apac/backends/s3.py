from __future__ import annotations
from typing import Any, Dict, List

from option import Result, Ok, Err
from mictlanx.apac.backends.base import AbstractStorageBackend


class S3Backend(AbstractStorageBackend):
    """
    Storage backend backed by an AWS S3 bucket.

    Requires: boto3 (`pip install boto3`)
    Each instance maps to a single (bucket, key_prefix) scope.
    """

    def __init__(
        self,
        backend_id: str,
        bucket_name: str,
        key_prefix: str = "",
        region_name: str = "us-east-1",
        aws_access_key_id: str = "",
        aws_secret_access_key: str = "",
        endpoint_url: str = "",
    ) -> None:
        self._id = backend_id
        self._bucket_name = bucket_name
        self._prefix = key_prefix.rstrip("/")
        self._region = region_name
        self._endpoint = endpoint_url or None

        try:
            import boto3
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id or None,
                aws_secret_access_key=aws_secret_access_key or None,
                region_name=region_name,
            )
            self._s3 = session.client("s3", endpoint_url=self._endpoint)
        except ImportError:
            raise ImportError("boto3 is required for S3Backend: pip install boto3")

    @property
    def backend_id(self) -> str:
        return self._id

    def _full_key(self, bucket_id: str, key: str) -> str:
        parts = [p for p in [self._prefix, bucket_id, key] if p]
        return "/".join(parts)

    async def put(
        self,
        bucket_id: str,
        key: str,
        value: bytes,
        tags: Dict[str, str] = {},
        content_type: str = "application/octet-stream",
    ) -> Result[Dict[str, Any], Exception]:
        try:
            full_key = self._full_key(bucket_id, key)
            tag_str = "&".join(f"{k}={v}" for k, v in tags.items()) if tags else ""
            kwargs: Dict[str, Any] = {
                "Bucket": self._bucket_name,
                "Key": full_key,
                "Body": value,
                "ContentType": content_type,
            }
            if tag_str:
                kwargs["Tagging"] = tag_str
            self._s3.put_object(**kwargs)
            return Ok({"bucket": self._bucket_name, "key": full_key})
        except Exception as e:
            return Err(e)

    async def get(self, bucket_id: str, key: str) -> Result[bytes, Exception]:
        try:
            full_key = self._full_key(bucket_id, key)
            response = self._s3.get_object(Bucket=self._bucket_name, Key=full_key)
            data: bytes = response["Body"].read()
            return Ok(data)
        except Exception as e:
            return Err(e)

    async def list(self, bucket_id: str) -> Result[List[str], Exception]:
        try:
            prefix = self._full_key(bucket_id, "") + "/"
            paginator = self._s3.get_paginator("list_objects_v2")
            keys: List[str] = []
            for page in paginator.paginate(Bucket=self._bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"].removeprefix(prefix))
            return Ok(keys)
        except Exception as e:
            return Err(e)

    async def delete(self, bucket_id: str, key: str) -> Result[bool, Exception]:
        try:
            full_key = self._full_key(bucket_id, key)
            self._s3.delete_object(Bucket=self._bucket_name, Key=full_key)
            return Ok(True)
        except Exception as e:
            return Err(e)

    async def health(self) -> bool:
        try:
            self._s3.head_bucket(Bucket=self._bucket_name)
            return True
        except Exception:
            return False
