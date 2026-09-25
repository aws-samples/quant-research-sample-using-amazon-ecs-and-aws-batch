"""Listing and completeness checks for model prefixes."""


def list_model_objects(client, bucket: str, prefix: str) -> dict:
    """{key: size} for every object under prefix (trailing slash enforced
    so 'model-a' never matches 'model-ab')."""
    prefix = prefix.rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    return {
        o["Key"]: o["Size"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for o in page.get("Contents", [])
    }


def replica_is_complete(source_objs: dict, replica_objs: dict) -> bool:
    """True when the replica holds every source key at the same size.
    Extra keys on the replica (e.g. stale versions pending lifecycle
    expiry) don't disqualify it — only missing/mismatched ones do."""
    if not source_objs:
        return False
    return all(replica_objs.get(k) == v for k, v in source_objs.items())
