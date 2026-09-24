"""Symbol-keyed S3 cache of derived peer sets.

Keyed by bare reporter symbol (not entity id) so a company reporting again —
this month or next quarter — reuses its basket without another LLM call.
Layout: s3://<bucket>/<prefix>/peers/cache/symbol=<SYM>.json
"""

import json
import logging
from typing import Optional

import boto3

logger = logging.getLogger(__name__)


class PeerCache:
    def __init__(self, bucket: str, prefix: str,
                 boto3_session: Optional[boto3.Session] = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = (boto3_session or boto3.Session()).client("s3")

    def _key(self, symbol: str) -> str:
        return f"{self.prefix}/peers/cache/symbol={symbol}.json"

    def get(self, symbol: str) -> Optional[dict]:
        try:
            body = self.s3.get_object(Bucket=self.bucket,
                                      Key=self._key(symbol))["Body"].read()
            return json.loads(body)
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.warning("peer cache read failed for %s: %s", symbol, e)
            return None

    def put(self, symbol: str, entry: dict) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self._key(symbol),
                           Body=json.dumps(entry, indent=2).encode("utf-8"),
                           ContentType="application/json")

    def delete(self, symbol: str) -> bool:
        """Remove a cached peer set so the next derive() recomputes it with the
        current universe filter. Returns True if a key was present."""
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=self._key(symbol))
            return True
        except Exception as e:
            logger.warning("peer cache delete failed for %s: %s", symbol, e)
            return False
