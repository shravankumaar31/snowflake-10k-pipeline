"""Burn rate and runway analysis for Snowflake financials."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from src.cloud.s3_uploader import S3Uploader
from src.common.config import load_config
from src.common.logging_utils import setup_logger



def compute_burn_and_runway(df: pd.DataFrame) -> pd.DataFrame:
    """Compute burn rate, runway months, and risk flags.

    Assumptions:
        - Operating cash flow is quarterly in USD millions.
        - Negative operating cash flow indicates cash burn.
        - Average monthly burn is quarterly burn divided by 3.
    """
    out = df.copy().sort_values("period_end")

    for col in ["OperatingCashFlow", "CashAndEquivalents"]:
        if col not in out.columns:
            out[col] = np.nan

    out["quarterly_cash_burn"] = np.where(out["OperatingCashFlow"] < 0, -out["OperatingCashFlow"], 0.0)
    out["avg_monthly_burn"] = out["quarterly_cash_burn"] / 3.0
    out["runway_months"] = np.where(
        out["avg_monthly_burn"] > 0,
        out["CashAndEquivalents"] / out["avg_monthly_burn"],
        np.inf,
    )
    out["runway_risk_lt_12m"] = out["runway_months"] < 12
    return out


def run_burn_rate_model(year: str | None = None) -> str:
    """Run burn rate model and upload outputs to S3."""
    config = load_config()
    logger = setup_logger("model_burn", "data/processed/burn_rate.log")
    uploader = S3Uploader(config.aws_region)

    if year is None:
        year = pd.Timestamp.utcnow().strftime("%Y")

    local_input = Path("data/processed/snowflake/10k") / year / "cleaned_financials.csv"
    if not (local_input.exists() and local_input.stat().st_size > 0):
        input_key = f"{config.processed_prefix}/{year}/cleaned_financials.csv"
        uploader.download_file(config.s3_bucket, input_key, str(local_input))

    df = pd.read_csv(local_input)
    result = compute_burn_and_runway(df)

    out_dir = Path("data/processed/snowflake/10k") / year
    out_dir.mkdir(parents=True, exist_ok=True)
    output_file = out_dir / "burn_rate_runway.csv"
    result.to_csv(output_file, index=False)

    key = f"{config.models_prefix}/{year}/burn_rate_runway.csv"
    try:
        uploader.upload_file(str(output_file), config.s3_bucket, key)
        logger.info("Uploaded burn/runway output to s3://%s/%s", config.s3_bucket, key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Upload failed for s3://%s/%s: %s", config.s3_bucket, key, exc)
    return str(output_file)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Run burn rate and runway model")
    parser.add_argument("--year", type=str, default=None)
    args = parser.parse_args()
    run_burn_rate_model(year=args.year)


if __name__ == "__main__":
    _cli()
