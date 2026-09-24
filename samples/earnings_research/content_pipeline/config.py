"""Pipeline configuration: dataclasses + loader supporting local and s3:// paths."""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AwsConfig:
    region: Optional[str] = None      # None -> settings aws.region
    profile: Optional[str] = None


@dataclass
class S3Config:
    bucket: Optional[str] = None   # None -> settings s3.data_bucket
    prefix: str = "earnings-content"


@dataclass
class RedshiftConfig:
    workgroup: Optional[str] = None   # None -> settings redshift.workgroup
    database: Optional[str] = None    # None -> settings redshift.database
    secret_arn: Optional[str] = None  # None -> settings redshift.secret_arn
    query_timeout_s: int = 1800
    row_limit: Optional[int] = None  # non-null = test slice


@dataclass
class ManifestConfig:
    num_shards: int = 200


@dataclass
class FetchConfig:
    worker_concurrency: int = 32
    per_domain_rps: float = 2.0
    per_domain_burst: int = 2
    domain_cooldown_s: float = 60.0
    connect_timeout_s: int = 10
    total_timeout_s: int = 60
    max_redirects: int = 10
    max_body_bytes: int = 50 * 1024 * 1024
    max_attempts: int = 3
    backoff_base_s: float = 2.0
    user_agent: Optional[str] = "quant-research-earnings-pipeline/1.0"  # None -> adds settings contact_email
    tls_insecure_fallback: bool = True
    retry_statuses_on_resume: List[str] = field(default_factory=list)
    checkpoint_every_rows: int = 500
    checkpoint_every_s: int = 300
    checkpoint_every_bytes: int = 256 * 1024 * 1024
    extract_threads: int = 4


@dataclass
class Config:
    job_name: str = "er-content"
    aws: AwsConfig = field(default_factory=AwsConfig)
    s3: S3Config = field(default_factory=S3Config)
    redshift: RedshiftConfig = field(default_factory=RedshiftConfig)
    manifest: ManifestConfig = field(default_factory=ManifestConfig)
    fetch: FetchConfig = field(default_factory=FetchConfig)

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        return cls(
            job_name=data.get("job_name", "er-content"),
            aws=AwsConfig(**data.get("aws", {})),
            s3=S3Config(**data.get("s3", {})),
            redshift=RedshiftConfig(**data.get("redshift", {})),
            manifest=ManifestConfig(**data.get("manifest", {})),
            fetch=FetchConfig(**data.get("fetch", {})),
        )


def apply_settings_fallbacks(cfg: Config) -> Config:
    """Fill account-specific values left null in the pipeline config from the
    root settings file. aws.profile is deliberately NOT filled: in-container runs
    must use the task role and local runs set AWS_PROFILE."""
    import settings

    if not cfg.aws.region:
        cfg.aws.region = settings.get("aws", "region")
    if not cfg.s3.bucket:
        cfg.s3.bucket = settings.get("s3", "data_bucket")
    if not cfg.redshift.workgroup:
        cfg.redshift.workgroup = settings.get("redshift", "workgroup")
    if not cfg.redshift.database:
        cfg.redshift.database = settings.get("redshift", "database")
    if not cfg.redshift.secret_arn:
        cfg.redshift.secret_arn = settings.get("redshift", "secret_arn")
    if cfg.fetch.user_agent is None:
        cfg.fetch.user_agent = (f"quant-research-earnings-pipeline/1.0 "
                                f"(contact: {settings.get('contact_email')})")
    return cfg


def load_config(path: str) -> Config:
    """Load config from a local file or an s3:// URI, then fill nulls from settings."""
    if path.startswith("s3://"):
        import boto3

        bucket, _, key = path[5:].partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        data = json.loads(body)
        logger.info("Loaded config from %s", path)
    else:
        with open(path) as f:
            data = json.load(f)
        logger.info("Loaded config from local file %s", path)
    return apply_settings_fallbacks(Config.from_dict(data))
