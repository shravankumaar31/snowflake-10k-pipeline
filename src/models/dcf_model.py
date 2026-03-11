"""DCF valuation model and sensitivity analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.cloud.s3_uploader import S3Uploader
from src.common.config import load_config
from src.common.logging_utils import setup_logger


def cagr(start_value: float, end_value: float, periods: int) -> float:
    """Calculate compound annual growth rate."""
    if start_value <= 0 or periods <= 0:
        return 0.0
    return (end_value / start_value) ** (1 / periods) - 1


def _load_cleaned_data(year: str, config: object, uploader: S3Uploader, logger: object) -> pd.DataFrame:
    local_input = Path("data/processed/snowflake/10k") / year / "cleaned_financials.csv"
    if local_input.exists() and local_input.stat().st_size > 0:
        logger.info("Using local cleaned dataset: %s", local_input)
        return pd.read_csv(local_input)

    input_key = f"{config.processed_prefix}/{year}/cleaned_financials.csv"
    uploader.download_file(config.s3_bucket, input_key, str(local_input))
    logger.info("Downloaded cleaned dataset from s3://%s/%s", config.s3_bucket, input_key)
    return pd.read_csv(local_input)


def _prepare_annual_fcf_series(df: pd.DataFrame) -> pd.Series:
    """Prefer FY free cash flow; fallback to last non-null values."""
    out = df.copy()
    out["period_end"] = pd.to_datetime(out.get("period_end"), errors="coerce")
    out["free_cash_flow"] = pd.to_numeric(out.get("free_cash_flow"), errors="coerce")
    out = out.dropna(subset=["period_end", "free_cash_flow"])

    if {"fiscal_period", "fiscal_year"}.issubset(out.columns):
        out["fiscal_year"] = pd.to_numeric(out["fiscal_year"], errors="coerce")
        fy = out[(out["fiscal_period"] == "FY") & (out["period_end"].dt.month == 1)].copy()
        fy = fy.dropna(subset=["fiscal_year"])
        if not fy.empty:
            fy["fiscal_year"] = fy["fiscal_year"].astype(int)
            fy = fy[fy["fiscal_year"] == fy["period_end"].dt.year]
            fy = fy.groupby("period_end", as_index=False)["free_cash_flow"].median().sort_values("period_end")
            if len(fy) >= 3:
                return fy["free_cash_flow"].reset_index(drop=True)

    # Fallback keeps recent values to avoid noisy long tails from mixed granularity.
    return out.sort_values("period_end")["free_cash_flow"].tail(8).reset_index(drop=True)


def project_fcf(history: pd.Series, years: int = 5) -> pd.Series:
    """Project free cash flow using bounded CAGR from recent history."""
    hist = history.dropna().astype(float)
    if len(hist) < 2:
        growth = 0.0
        base = float(hist.iloc[-1]) if len(hist) else 0.0
    else:
        lookback = min(4, len(hist))
        start = float(hist.iloc[-lookback])
        end = float(hist.iloc[-1])
        growth = cagr(start, end, lookback - 1)
        growth = float(np.clip(growth, -0.20, 0.35))
        base = float(hist.iloc[-1])

    projections = [base * ((1 + growth) ** i) for i in range(1, years + 1)]
    return pd.Series(projections, index=[f"Year{i}" for i in range(1, years + 1)], dtype=float)


def dcf_value(projected_fcf: pd.Series, wacc: float = 0.10, terminal_growth: float = 0.03) -> float:
    """Compute enterprise value from projected cash flows."""
    if wacc <= terminal_growth + 0.005:
        raise ValueError("wacc must be safely above terminal growth")

    discount_factors = np.array([(1 + wacc) ** i for i in range(1, len(projected_fcf) + 1)])
    pv_fcf = float(np.sum(projected_fcf.to_numpy() / discount_factors))

    terminal_fcf = float(projected_fcf.iloc[-1]) * (1 + terminal_growth)
    terminal_value = terminal_fcf / (wacc - terminal_growth)
    pv_terminal = terminal_value / ((1 + wacc) ** len(projected_fcf))

    return pv_fcf + pv_terminal


def sensitivity_matrix(projected_fcf: pd.Series) -> pd.DataFrame:
    """Build WACC x terminal growth sensitivity grid."""
    wacc_values = np.arange(0.08, 0.141, 0.01)
    growth_values = np.arange(0.01, 0.051, 0.01)

    rows = []
    for wacc in wacc_values:
        row = {}
        for growth in growth_values:
            if growth >= (wacc - 0.005):
                row[round(growth, 2)] = np.nan
            else:
                row[round(growth, 2)] = dcf_value(projected_fcf, float(wacc), float(growth))
        rows.append(row)

    matrix = pd.DataFrame(rows, index=[round(x, 2) for x in wacc_values])
    matrix.index.name = "wacc"
    matrix.columns.name = "terminal_growth"
    return matrix


def _safe_upload(uploader: S3Uploader, local_file: Path, bucket: str, key: str, logger: object) -> None:
    try:
        uploader.upload_file(str(local_file), bucket, key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Upload failed for s3://%s/%s: %s", bucket, key, exc)


def run_dcf(year: str | None = None, wacc: float = 0.10, terminal_growth: float = 0.03) -> str:
    """Run DCF valuation and export Excel with sensitivity."""
    config = load_config()
    logger = setup_logger("model_dcf", "data/processed/dcf.log")
    uploader = S3Uploader(config.aws_region)

    if year is None:
        year = pd.Timestamp.utcnow().strftime("%Y")

    df = _load_cleaned_data(year, config, uploader, logger).sort_values("period_end")
    fcf_history = _prepare_annual_fcf_series(df)
    projected = project_fcf(fcf_history, years=5)
    base_ev = dcf_value(projected, wacc=wacc, terminal_growth=terminal_growth)
    matrix = sensitivity_matrix(projected)

    out_dir = Path("data/processed/snowflake/10k") / year
    out_dir.mkdir(parents=True, exist_ok=True)

    xlsx_file = out_dir / "dcf_valuation.xlsx"
    with pd.ExcelWriter(xlsx_file, engine="xlsxwriter") as writer:
        fcf_history.to_frame("historical_fcf_musd").to_excel(writer, sheet_name="Historical_FCF", index=False)
        projected.to_frame("projected_fcf_musd").to_excel(writer, sheet_name="Projected_FCF")
        pd.DataFrame(
            [
                {
                    "base_case_ev_musd": base_ev,
                    "wacc": wacc,
                    "terminal_growth": terminal_growth,
                    "historical_points": len(fcf_history),
                }
            ]
        ).to_excel(writer, sheet_name="Base_Case", index=False)
        matrix.to_excel(writer, sheet_name="Sensitivity")

    heatmap_file = out_dir / "dcf_sensitivity_heatmap.png"
    _plot_heatmap(matrix, str(heatmap_file), base_wacc=wacc, base_growth=terminal_growth)

    xlsx_key = f"{config.models_prefix}/{year}/dcf_valuation.xlsx"
    heatmap_key = f"{config.models_prefix}/{year}/dcf_sensitivity_heatmap.png"
    _safe_upload(uploader, xlsx_file, config.s3_bucket, xlsx_key, logger)
    _safe_upload(uploader, heatmap_file, config.s3_bucket, heatmap_key, logger)
    logger.info("DCF complete with %s historical points", len(fcf_history))
    return str(xlsx_file)


def _plot_heatmap(matrix: pd.DataFrame, output_file: str, base_wacc: float, base_growth: float) -> None:
    plt.figure(figsize=(10, 7))
    data = matrix.values
    im = plt.imshow(data, cmap="YlOrBr", aspect="auto")

    plt.xticks(range(len(matrix.columns)), [f"{c:.0%}" for c in matrix.columns])
    plt.yticks(range(len(matrix.index)), [f"{i:.0%}" for i in matrix.index])
    plt.xlabel("Terminal Growth")
    plt.ylabel("WACC")
    plt.title("DCF Sensitivity (Enterprise Value, USD millions)")

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if np.isnan(val):
                continue
            color = "black" if val < np.nanmax(data) * 0.7 else "white"
            plt.text(j, i, f"{val:,.0f}", ha="center", va="center", fontsize=8, color=color)

    row_idx = int(np.argmin(np.abs(matrix.index.to_numpy(dtype=float) - base_wacc)))
    col_idx = int(np.argmin(np.abs(matrix.columns.to_numpy(dtype=float) - base_growth)))
    plt.scatter(col_idx, row_idx, marker="s", s=120, facecolors="none", edgecolors="black", linewidths=1.6, label="Base")

    cbar = plt.colorbar(im)
    cbar.set_label("Enterprise Value (USD millions)")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(output_file, dpi=180)
    plt.close()


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Run DCF valuation model")
    parser.add_argument("--year", type=str, default=None)
    parser.add_argument("--wacc", type=float, default=0.10)
    parser.add_argument("--terminal-growth", type=float, default=0.03)
    args = parser.parse_args()
    run_dcf(year=args.year, wacc=args.wacc, terminal_growth=args.terminal_growth)


if __name__ == "__main__":
    _cli()
