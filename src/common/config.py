"""Configuration helpers for the Snowflake 10-K pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration loaded from environment variables."""

    aws_region: str
    s3_bucket: str
    sec_user_agent: str
    company_ticker: str
    company_cik: str
    raw_prefix: str
    processed_prefix: str
    models_prefix: str
    logs_prefix: str



def load_config() -> PipelineConfig:
    """Load pipeline configuration from environment variables.

    Returns:
        PipelineConfig: Parsed configuration object.

    Raises:
        ValueError: If mandatory variables are missing.
    """
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket = os.getenv("S3_BUCKET", "")
    sec_user_agent = os.getenv("SEC_USER_AGENT", "")

    if not s3_bucket:
        raise ValueError("S3_BUCKET must be set")
    if not sec_user_agent:
        raise ValueError("SEC_USER_AGENT must be set for SEC API compliance")

    return PipelineConfig(
        aws_region=aws_region,
        s3_bucket=s3_bucket,
        sec_user_agent=sec_user_agent,
        company_ticker=os.getenv("COMPANY_TICKER", "SNOW"),
        company_cik=os.getenv("COMPANY_CIK", "0001640147"),
        raw_prefix=os.getenv("RAW_PREFIX", "raw/snowflake/10k"),
        processed_prefix=os.getenv("PROCESSED_PREFIX", "processed/snowflake/10k"),
        models_prefix=os.getenv("MODELS_PREFIX", "models/snowflake"),
        logs_prefix=os.getenv("LOGS_PREFIX", "logs/snowflake"),
    )
