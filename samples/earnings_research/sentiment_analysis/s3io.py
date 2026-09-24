"""Direct-to-S3 I/O for the study — polars write_*/read_* against s3:// URIs
(user directive 2026-08-04: no boto3 upload_file/get_object for datasets).

Tabular data goes through polars' native object-store path; binary artifacts
(PNG charts, .k tables, facts.json) go through fsspec streams — the same
s3:// URI convention, no local temp files, no explicit uploads.

Credential handling: options are resolved once from the boto3 profile chain
(supports SSO/temporary creds) and passed as storage_options — polars and
fsspec want different option shapes, hence the two dicts.
"""

from typing import Optional

import boto3
import fsspec
import polars as pl
from botocore.exceptions import ClientError

import settings


class S3IO:
    def __init__(self, profile: Optional[str] = None):
        """profile=None -> standard credential chain (env, SSO, or the Batch
        task role in-container)."""
        sess = boto3.Session(profile_name=profile) if profile else boto3.Session()
        region = settings.get("aws", "region")
        c = sess.get_credentials().get_frozen_credentials()
        self.pl_opts = {"aws_region": region, "aws_access_key_id": c.access_key,
                        "aws_secret_access_key": c.secret_key}
        self.fs_opts = {"key": c.access_key, "secret": c.secret_key,
                        "client_kwargs": {"region_name": region}}
        if c.token:
            self.pl_opts["aws_session_token"] = c.token
            self.fs_opts["token"] = c.token

    # ---- tabular (polars native)
    def write_parquet(self, df: pl.DataFrame, uri: str) -> None:
        df.write_parquet(uri, storage_options=self.pl_opts)

    def read_parquet(self, uri: str) -> pl.DataFrame:
        return pl.read_parquet(uri, storage_options=self.pl_opts)

    def write_csv(self, df: pl.DataFrame, uri: str) -> None:
        df.write_csv(uri, storage_options=self.pl_opts)

    def read_csv(self, uri: str) -> pl.DataFrame:
        return pl.scan_csv(uri, storage_options=self.pl_opts).collect()

    # ---- binary / text artifacts (fsspec stream on the same s3:// URIs)
    def write_bytes(self, data: bytes, uri: str) -> None:
        with fsspec.open(uri, "wb", **self.fs_opts) as f:
            f.write(data)

    def write_text(self, text: str, uri: str) -> None:
        self.write_bytes(text.encode("utf-8"), uri)

    def read_bytes(self, uri: str) -> bytes:
        with fsspec.open(uri, "rb", **self.fs_opts) as f:
            return f.read()

    def exists(self, uri: str) -> bool:
        """Check if S3 object exists (head_object). Re-raises on 403, timeout, etc."""
        s3 = boto3.client("s3", region_name=settings.get("aws", "region"),
                        aws_access_key_id=self.pl_opts["aws_access_key_id"],
                        aws_secret_access_key=self.pl_opts["aws_secret_access_key"],
                        aws_session_token=self.pl_opts.get("aws_session_token"))
        try:
            s3.head_object(Bucket=uri.split("/")[2], Key="/".join(uri.split("/")[3:]))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
