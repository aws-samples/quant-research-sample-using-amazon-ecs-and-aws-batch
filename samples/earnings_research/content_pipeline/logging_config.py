"""Logging configuration — structured format to stderr, noisy libs suppressed.

Adapted from aws_batch_inference_bedrock/logging_config.py with pipeline
context fields (shard, event_id).
"""

import logging
import sys


class ContextFilter(logging.Filter):
    """Ensure pipeline context fields exist on every record."""

    def filter(self, record):
        if not hasattr(record, "shard"):
            record.shard = ""
        if not hasattr(record, "event_id"):
            record.event_id = ""
        return True


def configure_logging(level: str = "INFO", shard: int = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    shard_tag = f" [shard {shard}]" if shard is not None else ""
    formatter = logging.Formatter(
        fmt=f"%(asctime)s - %(levelname)s - %(name)s{shard_tag} - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(ContextFilter())
    root_logger.addHandler(console_handler)

    for noisy in ("boto3", "botocore", "urllib3", "s3transfer", "aiohttp",
                  "trafilatura", "pypdf", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root_logger.info("Logging configured: level=%s", level)
