"""Redshift access for the manifest builder.

Adapted from mcp/data_catalog_mcp/redshift_utils.py: same
execute/poll/paginate flow via the redshift-data API with secret-ARN auth
(temporary IAM credentials fail on the datashare workgroup with a
ValidationException), but with unbounded pagination and a configurable
timeout — the manifest query returns ~1.1M rows.
"""

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional

import boto3

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = 3


def _cell(col: Dict[str, Any]) -> Any:
    if col.get("isNull"):
        return None
    for v in col.values():
        return v
    return None


class RedshiftClient:
    def __init__(self, workgroup: str, database: str, secret_arn: str,
                 region: str = "us-east-1",
                 boto3_session: Optional[boto3.Session] = None):
        self.workgroup = workgroup
        self.database = database
        self.secret_arn = secret_arn
        sess = boto3_session or boto3.Session()
        self.client = sess.client("redshift-data", region_name=region)

    def execute(self, sql: str, timeout_s: int = 1800) -> str:
        """Run SQL to completion; returns the statement id."""
        stmt = self.client.execute_statement(
            WorkgroupName=self.workgroup,
            Database=self.database,
            SecretArn=self.secret_arn,
            Sql=sql,
        )
        sid = stmt["Id"]
        deadline = time.time() + timeout_s
        while True:
            desc = self.client.describe_statement(Id=sid)
            status = desc["Status"]
            if status == "FINISHED":
                return sid
            if status in ("FAILED", "ABORTED"):
                raise RuntimeError(f"query {status}: {desc.get('Error')}")
            if time.time() > deadline:
                self.client.cancel_statement(Id=sid)
                raise TimeoutError(f"query timed out after {timeout_s}s")
            time.sleep(_POLL_INTERVAL_S)

    def fetch_all(self, sql: str, timeout_s: int = 1800,
                  progress_cb: Optional[Callable[[int], None]] = None,
                  ) -> Iterator[Dict[str, Any]]:
        """Run SQL and yield every result row as a dict (full pagination)."""
        sid = self.execute(sql, timeout_s=timeout_s)
        columns: List[str] = []
        token: Optional[str] = None
        total = 0
        page_num = 0
        while True:
            kwargs = {"Id": sid}
            if token:
                kwargs["NextToken"] = token
            page = self.client.get_statement_result(**kwargs)
            if not columns:
                columns = [c["name"] for c in page.get("ColumnMetadata", [])]
            for rec in page.get("Records", []):
                total += 1
                yield dict(zip(columns, (_cell(c) for c in rec)))
            page_num += 1
            if progress_cb and page_num % 50 == 0:
                progress_cb(total)
            token = page.get("NextToken")
            if not token:
                break
        logger.info("fetched %d rows in %d pages", total, page_num)
