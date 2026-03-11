"""Data cleaning and feature engineering pipeline for Snowflake 10-K data."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.cloud.s3_uploader import S3Uploader
from src.common.config import load_config
from src.common.logging_utils import setup_logger


def to_snake_case(value: str) -> str:
    """Convert a string to snake_case."""
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_").lower()


def parse_financial_value(value: Any) -> float:
    """Parse financial strings into float values in millions USD.

    Handles examples like "(1,234)", "$1.2B", "$350M".
    """
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value) / 1_000_000.0

    raw = str(value).strip().replace("$", "").replace(",", "")
    negative = raw.startswith("(") and raw.endswith(")")
    raw = raw.strip("()")

    multiplier = 1.0
    if raw.endswith("B"):
        multiplier = 1_000.0
        raw = raw[:-1]
    elif raw.endswith("M"):
        multiplier = 1.0
        raw = raw[:-1]
    elif raw.endswith("K"):
        multiplier = 0.001
        raw = raw[:-1]

    try:
        parsed = float(raw) * multiplier
        return -parsed if negative else parsed
    except ValueError:
        return np.nan


def standardize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize columns and parse dates."""
    out = df.copy()
    out.columns = [to_snake_case(c) for c in out.columns]

    if "end_date" in out.columns:
        out["period_end"] = pd.to_datetime(out["end_date"], errors="coerce")
    elif "period_end" in out.columns:
        out["period_end"] = pd.to_datetime(out["period_end"], errors="coerce")

    for col in ["fiscal_year", "fiscal_period"]:
        if col not in out.columns:
            out[col] = pd.NA

    if "value" in out.columns:
        out["value_musd"] = out["value"].apply(parse_financial_value)

    out["is_imputed"] = False
    return out


def build_statement_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long statement data into one row per period."""
    required = {"period_end", "metric", "value_musd"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required - set(df.columns)}")

    pivot = (
        df.pivot_table(
            index=["period_end", "fiscal_year", "fiscal_period"],
            columns="metric",
            values="value_musd",
            aggfunc="last",
        )
        .reset_index()
        .sort_values("period_end")
    )
    pivot.columns.name = None
    return pivot


def impute_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill numeric columns and mark imputed rows."""
    out = df.copy()
    numeric_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    missing_before = out[numeric_cols].isna()
    out[numeric_cols] = out[numeric_cols].ffill()
    missing_after = out[numeric_cols].isna()
    imputed_cells = missing_before & ~missing_after
    out["is_imputed"] = imputed_cells.any(axis=1)
    return out


def flag_yoy_outliers(df: pd.DataFrame, metric_cols: list[str]) -> pd.DataFrame:
    """Flag YoY changes above 200% absolute value."""
    out = df.copy().sort_values("period_end")
    out["yoy_outlier_flag"] = False

    for col in metric_cols:
        if col not in out.columns:
            continue
        yoy = out[col].pct_change(periods=4)
        out["yoy_outlier_flag"] = out["yoy_outlier_flag"] | (yoy.abs() > 2.0)
    return out


def apply_accounting_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Add accounting reconciliation checks."""
    out = df.copy()
    tol = 1e-2

    for needed in ["TotalAssets", "TotalLiabilities", "StockholdersEquity", "NetIncome", "OperatingCashFlow"]:
        if needed not in out.columns:
            out[needed] = np.nan

    out["balance_sheet_check_pass"] = (
        (out["TotalAssets"] - (out["TotalLiabilities"] + out["StockholdersEquity"]))
        .abs()
        .fillna(np.inf)
        <= tol
    )

    out["net_income_vs_ocf_flag"] = (
        (out["NetIncome"] - out["OperatingCashFlow"]).abs() > (0.5 * out["NetIncome"].abs().replace(0, 1.0))
    )
    return out


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived financial features required for analysis."""
    out = df.copy().sort_values("period_end")

    safe_div = lambda a, b: np.where((b == 0) | pd.isna(b), np.nan, a / b)

    for col in ["Revenue", "COGS", "OperatingLoss", "OperatingCashFlow", "CapEx", "CurrentAssets", "CurrentLiabilities", "TotalDebt", "StockholdersEquity"]:
        if col not in out.columns:
            out[col] = np.nan

    out["gross_margin_pct"] = safe_div(out["Revenue"] - out["COGS"], out["Revenue"]) * 100
    out["operating_loss_margin_pct"] = safe_div(out["OperatingLoss"], out["Revenue"]) * 100
    out["revenue_growth_yoy_pct"] = out["Revenue"].pct_change(periods=4) * 100
    out["free_cash_flow"] = out["OperatingCashFlow"] - out["CapEx"]
    out["current_ratio"] = safe_div(out["CurrentAssets"], out["CurrentLiabilities"])
    out["debt_to_equity_ratio"] = safe_div(out["TotalDebt"], out["StockholdersEquity"])

    return out


def quality_report(df: pd.DataFrame) -> dict[str, Any]:
    """Build compact quality report."""
    null_rate = (df.isna().sum() / len(df)).sort_values(ascending=False).to_dict() if len(df) else {}
    report = {
        "row_count": int(len(df)),
        "null_rates": {k: round(float(v), 4) for k, v in null_rate.items()},
        "balance_check_pass_rate": round(float(df["balance_sheet_check_pass"].mean()), 4) if len(df) else 0.0,
        "imputed_row_count": int(df.get("is_imputed", pd.Series(dtype=bool)).sum()) if len(df) else 0,
    }
    return report


def run_cleaning(year: str | None = None) -> tuple[pd.DataFrame, dict[str, Any], str]:
    """Run full cleaning pipeline for statement CSVs and upload to S3."""
    config = load_config()
    logger = setup_logger("cleaning", "data/processed/cleaning.log")
    uploader = S3Uploader(config.aws_region)

    if year is None:
        year = pd.Timestamp.utcnow().strftime("%Y")

    local_raw_dir = Path("data/raw/snowflake/10k") / year
    local_raw_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "income": "income_statement.csv",
        "balance": "balance_sheet.csv",
        "cash": "cash_flow.csv",
    }

    raw_frames: dict[str, pd.DataFrame] = {}
    for name, filename in files.items():
        s3_key = f"{config.raw_prefix}/{year}/{filename}"
        local_path = local_raw_dir / filename
        logger.info("Downloading raw file s3://%s/%s", config.s3_bucket, s3_key)
        uploader.download_file(config.s3_bucket, s3_key, str(local_path))

        frame = pd.read_csv(local_path)
        frame = standardize_frame(frame)
        raw_frames[name] = frame

    income_wide = build_statement_wide(raw_frames["income"])
    balance_wide = build_statement_wide(raw_frames["balance"])
    cash_wide = build_statement_wide(raw_frames["cash"])

    merged = income_wide.merge(balance_wide, on=["period_end", "fiscal_year", "fiscal_period"], how="outer")
    merged = merged.merge(cash_wide, on=["period_end", "fiscal_year", "fiscal_period"], how="outer")
    merged = merged.sort_values("period_end")

    merged = impute_numeric(merged)
    metric_cols = [c for c in ["Revenue", "NetIncome", "OperatingCashFlow", "TotalAssets"] if c in merged.columns]
    merged = flag_yoy_outliers(merged, metric_cols)
    merged = apply_accounting_checks(merged)
    merged = engineer_features(merged)

    report = quality_report(merged)
    logger.info("Data quality report: %s", report)

    processed_dir = Path("data/processed/snowflake/10k") / year
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_file = processed_dir / "cleaned_financials.csv"
    merged.to_csv(output_file, index=False)

    output_key = f"{config.processed_prefix}/{year}/cleaned_financials.csv"
    uploader.upload_file(str(output_file), config.s3_bucket, output_key)
    logger.info("Uploaded cleaned dataset to s3://%s/%s", config.s3_bucket, output_key)

    report_file = processed_dir / "data_quality_report.json"
    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_key = f"{config.processed_prefix}/{year}/data_quality_report.json"
    uploader.upload_file(str(report_file), config.s3_bucket, report_key)

    print("Data Quality Report")
    print(report)
    return merged, report, str(output_file)


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Clean Snowflake 10-K raw statement data")
    parser.add_argument("--year", type=str, default=None, help="Year partition to process")
    args = parser.parse_args()
    run_cleaning(year=args.year)


if __name__ == "__main__":
    _cli()
