"""Revenue forecasting models with model scoring and cleaner quarterly series."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.arima.model import ARIMA

from src.cloud.s3_uploader import S3Uploader
from src.common.config import load_config
from src.common.logging_utils import setup_logger

try:
    from prophet import Prophet
except ImportError:  # pragma: no cover
    Prophet = None


MIN_POINTS_FOR_PROPHET = 12
FISCAL_QUARTER_FREQ = "QE-JAN"


def _future_quarter_ends(last_period_end: pd.Timestamp, periods: int) -> pd.DatetimeIndex:
    return pd.date_range(last_period_end, periods=periods + 1, freq=FISCAL_QUARTER_FREQ)[1:]


def _build_quarterly_metric(series_df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Build a quarter-only series and derive Q4 from FY - (Q1+Q2+Q3)."""
    month_for_period = {"Q1": 4, "Q2": 7, "Q3": 10}
    q123_parts: list[pd.DataFrame] = []

    for fiscal_period, month in month_for_period.items():
        part = series_df[
            (series_df["fiscal_period"] == fiscal_period) & (series_df["period_end"].dt.month == month)
        ].copy()
        part = part[part["fiscal_year"] == part["period_end"].dt.year + 1]
        if part.empty:
            continue
        part = part.groupby(["fiscal_year", "period_end"], as_index=False)[value_col].median()
        part["fiscal_period"] = fiscal_period
        q123_parts.append(part)

    if not q123_parts:
        return pd.DataFrame(columns=["period_end", value_col])

    q123 = pd.concat(q123_parts, ignore_index=True)
    q123 = q123.sort_values("period_end").groupby(["fiscal_year", "fiscal_period"], as_index=False).tail(1)

    fy = series_df[(series_df["fiscal_period"] == "FY") & (series_df["period_end"].dt.month == 1)].copy()
    fy = fy[fy["fiscal_year"] == fy["period_end"].dt.year]
    fy = fy.groupby(["fiscal_year", "period_end"], as_index=False)[value_col].median()
    fy = fy.rename(columns={value_col: "fy_value"})

    q4 = pd.DataFrame(columns=["fiscal_year", "period_end", "fiscal_period", value_col])
    pivot = q123.pivot_table(index="fiscal_year", columns="fiscal_period", values=value_col, aggfunc="last")
    if {"Q1", "Q2", "Q3"}.issubset(set(pivot.columns)):
        q4 = fy.merge(pivot[["Q1", "Q2", "Q3"]], left_on="fiscal_year", right_index=True, how="left")
        q4[value_col] = q4["fy_value"] - (q4["Q1"] + q4["Q2"] + q4["Q3"])
        q4 = q4[q4[value_col].notna() & q4[value_col].gt(0.0)]
        q4["fiscal_period"] = "Q4"
        q4 = q4[["fiscal_year", "period_end", "fiscal_period", value_col]]

    combined = pd.concat([q123, q4], ignore_index=True)
    combined = combined.sort_values("period_end")
    combined = combined.groupby("period_end", as_index=False)[value_col].median().sort_values("period_end")
    return combined


def _prepare_revenue_series(df: pd.DataFrame) -> pd.DataFrame:
    """Build a stable quarterly revenue series for forecasting."""
    out = df.copy()
    out["period_end"] = pd.to_datetime(out.get("period_end"), errors="coerce")
    out["Revenue"] = pd.to_numeric(out.get("Revenue"), errors="coerce")
    out = out.dropna(subset=["period_end", "Revenue"])

    if {"fiscal_period", "fiscal_year"}.issubset(out.columns):
        out["fiscal_year"] = pd.to_numeric(out["fiscal_year"], errors="coerce")
        with_fp = out.dropna(subset=["fiscal_period", "fiscal_year"]).copy()
        with_fp["fiscal_year"] = with_fp["fiscal_year"].astype(int)

        quarterly = _build_quarterly_metric(with_fp, "Revenue")
        if len(quarterly) >= 8:
            return _regularize_quarterly_series(quarterly[["period_end", "Revenue"]])

    # Fallback: drop FY and collapse to one point per quarter end.
    no_fy = out.copy()
    if "fiscal_period" in no_fy.columns:
        no_fy = no_fy[no_fy["fiscal_period"] != "FY"]
    no_fy = no_fy.groupby("period_end", as_index=False)["Revenue"].median().sort_values("period_end")
    return _regularize_quarterly_series(no_fy[["period_end", "Revenue"]])


def _regularize_quarterly_series(series: pd.DataFrame) -> pd.DataFrame:
    """Reindex to quarter-end cadence and interpolate missing quarters."""
    out = series.copy().dropna(subset=["period_end", "Revenue"]).sort_values("period_end")
    out["Revenue"] = pd.to_numeric(out["Revenue"], errors="coerce")
    out = out.dropna(subset=["Revenue"])
    out = out.groupby("period_end", as_index=False)["Revenue"].median().sort_values("period_end")
    if out.empty:
        return out

    full_quarter_index = pd.date_range(out["period_end"].min(), out["period_end"].max(), freq=FISCAL_QUARTER_FREQ)
    regular = out.set_index("period_end").reindex(full_quarter_index)
    regular["Revenue"] = regular["Revenue"].interpolate(limit_direction="both")
    regular = regular.reset_index().rename(columns={"index": "period_end"})
    return regular


def _non_negative_bounds(mean: np.ndarray, std: np.ndarray | float, z: float = 1.96) -> tuple[np.ndarray, np.ndarray]:
    if isinstance(std, np.ndarray):
        lower = np.maximum(mean - z * std, 0.0)
        upper = mean + z * std
        return lower, upper
    lower = np.maximum(mean - z * std, 0.0)
    upper = mean + z * std
    return lower, upper


def linear_regression_forecast(series: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    """Forecast revenue using log-linear regression on a time index."""
    y = np.clip(series["Revenue"].to_numpy(dtype=float), a_min=0.0, a_max=None)
    y_log = np.log1p(y)
    x = np.arange(len(y_log)).reshape(-1, 1)

    model = LinearRegression()
    model.fit(x, y_log)

    future_x = np.arange(len(y_log), len(y_log) + periods).reshape(-1, 1)
    pred_log = model.predict(future_x)

    fitted_log = model.predict(x)
    std_log = float(np.std(y_log - fitted_log)) if len(y_log) > 1 else 0.0

    preds = np.expm1(pred_log)
    lower = np.maximum(np.expm1(pred_log - 1.96 * std_log), 0.0)
    upper = np.expm1(pred_log + 1.96 * std_log)

    future_dates = _future_quarter_ends(series["period_end"].max(), periods)
    return pd.DataFrame(
        {
            "period_end": future_dates,
            "model": "linear_regression",
            "forecast_revenue": preds,
            "lower_ci": lower,
            "upper_ci": upper,
        }
    )


def _naive_forecast(series: pd.DataFrame, model_name: str, periods: int) -> pd.DataFrame:
    """Fallback forecast that repeats the latest value."""
    last_value = float(series["Revenue"].iloc[-1])
    future_dates = _future_quarter_ends(series["period_end"].max(), periods)
    arr = np.full(periods, last_value, dtype=float)
    return pd.DataFrame(
        {
            "period_end": future_dates,
            "model": model_name,
            "forecast_revenue": arr,
            "lower_ci": np.maximum(arr * 0.85, 0.0),
            "upper_ci": arr * 1.15,
        }
    )


def arima_forecast(series: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    """Forecast revenue using ARIMA(1,1,1) in log-space."""
    y = np.clip(series["Revenue"].astype(float).to_numpy(), a_min=0.0, a_max=None)
    if len(y) < 6:
        return _naive_forecast(series, "arima", periods)

    y_log = np.log1p(y)
    try:
        model = ARIMA(y_log, order=(1, 1, 1), enforce_stationarity=False, enforce_invertibility=False)
        fit = model.fit()
        fc = fit.get_forecast(steps=periods)
        mean = np.expm1(np.clip(fc.predicted_mean, -20.0, 20.0))
        ci_log = np.clip(fc.conf_int(alpha=0.05), -20.0, 20.0)
        ci = np.expm1(ci_log)
        lower = np.maximum(ci[:, 0], 0.0)
        upper = ci[:, 1]
    except Exception:  # noqa: BLE001
        return _naive_forecast(series, "arima", periods)

    future_dates = _future_quarter_ends(series["period_end"].max(), periods)
    return pd.DataFrame(
        {
            "period_end": future_dates,
            "model": "arima",
            "forecast_revenue": mean,
            "lower_ci": lower,
            "upper_ci": upper,
        }
    )


def prophet_forecast(series: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    """Forecast revenue using constrained Prophet."""
    if Prophet is None:
        raise ImportError("prophet is not installed")
    if len(series) < MIN_POINTS_FOR_PROPHET:
        raise ImportError(f"prophet requires at least {MIN_POINTS_FOR_PROPHET} observations")

    df = series.rename(columns={"period_end": "ds", "Revenue": "y"}).copy()
    cap = max(float(df["y"].max()) * 2.0, float(df["y"].max()) + 1.0)
    df["floor"] = 0.0
    df["cap"] = cap

    model = Prophet(
        growth="logistic",
        interval_width=0.9,
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.05,
        weekly_seasonality=False,
        daily_seasonality=False,
        yearly_seasonality=False,
    )
    model.add_seasonality(name="quarterly", period=365.25 / 4.0, fourier_order=5)
    model.fit(df)

    future = model.make_future_dataframe(periods=periods, freq=FISCAL_QUARTER_FREQ)
    future["floor"] = 0.0
    future["cap"] = cap
    forecast = model.predict(future).tail(periods)

    return pd.DataFrame(
        {
            "period_end": pd.to_datetime(forecast["ds"]),
            "model": "prophet",
            "forecast_revenue": np.maximum(forecast["yhat"], 0.0),
            "lower_ci": np.maximum(forecast["yhat_lower"], 0.0),
            "upper_ci": np.maximum(forecast["yhat_upper"], 0.0),
        }
    )


def _load_cleaned_data(year: str, config: object, uploader: S3Uploader, logger: object) -> pd.DataFrame:
    local_input = Path("data/processed/snowflake/10k") / year / "cleaned_financials.csv"
    if local_input.exists() and local_input.stat().st_size > 0:
        logger.info("Using local cleaned dataset: %s", local_input)
        return pd.read_csv(local_input)

    input_key = f"{config.processed_prefix}/{year}/cleaned_financials.csv"
    uploader.download_file(config.s3_bucket, input_key, str(local_input))
    logger.info("Downloaded cleaned dataset from s3://%s/%s", config.s3_bucket, input_key)
    return pd.read_csv(local_input)


def _safe_upload(uploader: S3Uploader, local_file: Path, bucket: str, key: str, logger: object) -> None:
    try:
        uploader.upload_file(str(local_file), bucket, key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Upload failed for s3://%s/%s: %s", bucket, key, exc)


def _error_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    mask = np.isfinite(actual) & np.isfinite(predicted)
    if not mask.any():
        return {"mae": float("nan"), "rmse": float("nan"), "mape_pct": float("nan"), "smape_pct": float("nan")}
    actual = actual[mask]
    predicted = predicted[mask]

    eps = 1e-9
    mae = float(np.mean(np.abs(actual - predicted)))
    rmse = float(np.sqrt(np.mean((actual - predicted) ** 2)))
    mape = float(np.mean(np.abs((actual - predicted) / np.maximum(np.abs(actual), eps))) * 100.0)
    smape = float(
        np.mean((2.0 * np.abs(predicted - actual)) / np.maximum(np.abs(actual) + np.abs(predicted), eps)) * 100.0
    )
    return {"mae": mae, "rmse": rmse, "mape_pct": mape, "smape_pct": smape}


def _evaluate_models(
    series: pd.DataFrame,
    periods: int,
    model_fns: dict[str, Callable[[pd.DataFrame, int], pd.DataFrame]],
    logger: object,
) -> pd.DataFrame:
    """Holdout evaluation using the most recent points."""
    if len(series) < 10:
        return pd.DataFrame(columns=["model", "mae", "rmse", "mape_pct", "smape_pct", "holdout_points"])

    holdout = min(periods, max(2, len(series) // 5))
    train = series.iloc[:-holdout].copy()
    test = series.iloc[-holdout:].copy()

    rows: list[dict[str, float | int | str]] = []
    for model_name, fn in model_fns.items():
        try:
            pred = fn(train, holdout)
            merged = test.merge(pred[["period_end", "forecast_revenue"]], on="period_end", how="inner")
            if merged.empty:
                continue
            metrics = _error_metrics(
                merged["Revenue"].to_numpy(dtype=float),
                merged["forecast_revenue"].to_numpy(dtype=float),
            )
            if not np.isfinite(metrics["smape_pct"]):
                continue
            rows.append({"model": model_name, **metrics, "holdout_points": int(len(merged))})
        except Exception as exc:  # noqa: BLE001
            logger.warning("Model evaluation failed for %s: %s", model_name, exc)

    return pd.DataFrame(rows).sort_values("smape_pct") if rows else pd.DataFrame()


def run_revenue_forecasting(year: str | None = None, periods: int = 4) -> str:
    """Run revenue forecasting models, score them, and export outputs."""
    config = load_config()
    logger = setup_logger("model_revenue", "data/processed/revenue_forecast.log")
    uploader = S3Uploader(config.aws_region)

    if year is None:
        year = pd.Timestamp.utcnow().strftime("%Y")

    df = _load_cleaned_data(year, config, uploader, logger)
    series = _prepare_revenue_series(df)
    if len(series) < 8:
        raise ValueError("Insufficient quarterly revenue observations for forecasting")

    model_fns: dict[str, Callable[[pd.DataFrame, int], pd.DataFrame]] = {
        "linear_regression": linear_regression_forecast,
        "arima": arima_forecast,
    }
    if Prophet is not None:
        model_fns["prophet"] = prophet_forecast

    forecasts: list[pd.DataFrame] = []
    for name, fn in model_fns.items():
        try:
            forecasts.append(fn(series, periods))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s forecast: %s", name, exc)

    if not forecasts:
        raise RuntimeError("No forecast model produced output")

    forecast_df = pd.concat(forecasts, ignore_index=True)
    metrics_df = _evaluate_models(series, periods, model_fns, logger)
    best_model = metrics_df.iloc[0]["model"] if not metrics_df.empty else None
    forecast_df["is_best_model"] = forecast_df["model"].eq(best_model) if best_model else False
    forecast_df["forecast_revenue"] = np.maximum(forecast_df["forecast_revenue"], 0.0)
    forecast_df["lower_ci"] = np.maximum(forecast_df["lower_ci"], 0.0)
    forecast_df["upper_ci"] = np.maximum(forecast_df["upper_ci"], 0.0)

    output_dir = Path("data/processed/snowflake/10k") / year
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "forecast_revenue.csv"
    forecast_df.to_csv(output_file, index=False)

    metrics_file = output_dir / "forecast_metrics.csv"
    if not metrics_df.empty:
        metrics_df.to_csv(metrics_file, index=False)

    plot_path = output_dir / "forecast_revenue.png"
    _plot_forecast(series, forecast_df, str(plot_path), best_model=best_model, metrics=metrics_df)

    csv_key = f"{config.models_prefix}/{year}/forecast_revenue.csv"
    png_key = f"{config.models_prefix}/{year}/forecast_revenue.png"
    _safe_upload(uploader, output_file, config.s3_bucket, csv_key, logger)
    _safe_upload(uploader, plot_path, config.s3_bucket, png_key, logger)
    if metrics_file.exists():
        metrics_key = f"{config.models_prefix}/{year}/forecast_metrics.csv"
        _safe_upload(uploader, metrics_file, config.s3_bucket, metrics_key, logger)

    logger.info("Revenue forecast complete. Best model: %s", best_model if best_model else "n/a")
    return str(output_file)


def _plot_forecast(
    actual: pd.DataFrame,
    forecast: pd.DataFrame,
    output_file: str,
    best_model: str | None = None,
    metrics: pd.DataFrame | None = None,
) -> None:
    plt.figure(figsize=(12, 7))
    actual_tail = actual.tail(16)
    plt.plot(actual_tail["period_end"], actual_tail["Revenue"], label="Actual", color="navy", linewidth=2.5)

    for model_name, model_df in forecast.groupby("model"):
        is_best = best_model is not None and model_name == best_model
        plt.plot(
            model_df["period_end"],
            model_df["forecast_revenue"],
            label=f"Forecast - {model_name}",
            linewidth=2.4 if is_best else 1.8,
            linestyle="-" if is_best else "--",
        )
        plt.fill_between(
            model_df["period_end"],
            model_df["lower_ci"],
            model_df["upper_ci"],
            alpha=0.22 if is_best else 0.10,
        )

    plt.title("Revenue Forecast (Quarterly): Actual vs Forecast")
    plt.xlabel("Quarter End")
    plt.ylabel("Revenue (USD millions)")
    plt.grid(alpha=0.2)
    plt.ylim(bottom=0.0)

    if metrics is not None and not metrics.empty:
        summary = "\n".join(
            [f"{row.model}: sMAPE {row.smape_pct:.1f}% | RMSE {row.rmse:.0f}" for row in metrics.head(3).itertuples()]
        )
        plt.gca().text(
            0.02,
            0.98,
            f"Holdout Metrics\n{summary}",
            transform=plt.gca().transAxes,
            va="top",
            fontsize=9,
            bbox={"boxstyle": "round,pad=0.3", "facecolor": "white", "alpha": 0.8},
        )

    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(output_file, dpi=180)
    plt.close()


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Run revenue forecasting models")
    parser.add_argument("--year", type=str, default=None)
    parser.add_argument("--periods", type=int, default=4)
    args = parser.parse_args()
    run_revenue_forecasting(year=args.year, periods=args.periods)


if __name__ == "__main__":
    _cli()
