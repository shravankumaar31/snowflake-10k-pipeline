"""Lambda entrypoint to orchestrate end-to-end pipeline."""

from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone

import boto3

from src.cleaning.cleaner import run_cleaning
from src.common.config import load_config
from src.common.logging_utils import setup_logger
from src.ingestion.sec_downloader import run_ingestion
from src.models.burn_rate import run_burn_rate_model
from src.models.dcf_model import run_dcf
from src.models.revenue_forecast import run_revenue_forecasting


def _publish_sns(message: str, subject: str) -> None:
    topic_arn = os.getenv("SNS_TOPIC_ARN", "")
    if not topic_arn:
        return

    sns = boto3.client("sns", region_name=os.getenv("AWS_REGION", "us-east-1"))
    sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)


def lambda_handler(event: dict, context: object) -> dict:
    """Run ingestion, cleaning, and modeling steps in sequence."""
    logger = setup_logger("lambda_pipeline")
    config = load_config()
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        logger.info("Pipeline started at %s", run_ts)

        ingestion_paths = run_ingestion()
        year = max(path.split("/")[-2] for path in ingestion_paths.values())

        _, quality, cleaned_file = run_cleaning(year=year)
        forecast_file = run_revenue_forecasting(year=year, periods=4)
        dcf_file = run_dcf(year=year)
        burn_file = run_burn_rate_model(year=year)

        summary = {
            "status": "SUCCESS",
            "run_timestamp": run_ts,
            "year": year,
            "s3_bucket": config.s3_bucket,
            "outputs": {
                "cleaned_financials": f"s3://{config.s3_bucket}/{config.processed_prefix}/{year}/cleaned_financials.csv",
                "forecast_revenue": f"s3://{config.s3_bucket}/{config.models_prefix}/{year}/forecast_revenue.csv",
                "dcf_valuation": f"s3://{config.s3_bucket}/{config.models_prefix}/{year}/dcf_valuation.xlsx",
                "burn_rate": f"s3://{config.s3_bucket}/{config.models_prefix}/{year}/burn_rate_runway.csv",
            },
            "local_outputs": {
                "cleaned_file": cleaned_file,
                "forecast_file": forecast_file,
                "dcf_file": dcf_file,
                "burn_file": burn_file,
            },
            "quality": quality,
        }

        _publish_sns(json.dumps(summary, indent=2), "Snowflake 10-K Pipeline Success")
        return summary

    except Exception as exc:  # noqa: BLE001
        failure = {
            "status": "FAILED",
            "run_timestamp": run_ts,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
        logger.error("Pipeline failed: %s", failure)
        _publish_sns(json.dumps(failure, indent=2), "Snowflake 10-K Pipeline Failure")
        raise


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, object()), indent=2))
