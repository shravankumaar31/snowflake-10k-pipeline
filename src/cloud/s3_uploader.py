"""S3 upload and download helpers."""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class S3Uploader:
    """Wrapper for S3 operations used by the pipeline."""

    def __init__(self, region_name: str) -> None:
        self.s3_client = boto3.client("s3", region_name=region_name)

    def upload_file(self, file_path: str, bucket: str, key: str) -> None:
        """Upload local file to S3.

        Args:
            file_path: Local path.
            bucket: S3 bucket name.
            key: S3 object key.
        """
        path_obj = Path(file_path)
        if not path_obj.exists():
            raise FileNotFoundError(f"Local file not found: {file_path}")

        try:
            self.s3_client.upload_file(str(path_obj), bucket, key)
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Failed uploading to s3://{bucket}/{key}") from exc

    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        """Download S3 object to local path."""
        path_obj = Path(local_path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.s3_client.download_file(bucket, key, str(path_obj))
        except (BotoCoreError, ClientError) as exc:
            raise RuntimeError(f"Failed downloading s3://{bucket}/{key}") from exc
