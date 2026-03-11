# Snowflake 10K Pipeline Runbook

## 0) Prerequisites
- Region: `us-west-1`
- Python env exists at `.venv`
- `.env` is configured (`S3_BUCKET`, `RAW_PREFIX`, `PROCESSED_PREFIX`, `MODELS_PREFIX`, AWS creds)
- Athena DB: `snowflake_tableau`
- Glue crawler has already created tables (e.g. `tbl_2025_*`)

---

## 1) Pipeline Run Commands (Local)

```bash
cd "/Users/shravankumaar/Documents/New project/snowflake-10k-pipeline"
source .venv/bin/activate
set -a && source .env && set +a
export PYTHONPATH=.
export MPLCONFIGDIR=/tmp/matplotlib-cache
mkdir -p "$MPLCONFIGDIR"

YEAR=2025

python -m src.models.revenue_forecast --year "$YEAR" --periods 4
python -m src.models.dcf_model --year "$YEAR" --wacc 0.10 --terminal-growth 0.03
python -m src.models.burn_rate --year "$YEAR"

LOCAL_DIR="data/processed/snowflake/10k/$YEAR"
S3_MODELS_DIR="s3://$S3_BUCKET/$MODELS_PREFIX/$YEAR"

aws s3 cp "$LOCAL_DIR/forecast_revenue.csv"        "$S3_MODELS_DIR/forecast_revenue.csv"
aws s3 cp "$LOCAL_DIR/forecast_metrics.csv"        "$S3_MODELS_DIR/forecast_metrics.csv"
aws s3 cp "$LOCAL_DIR/forecast_revenue.png"        "$S3_MODELS_DIR/forecast_revenue.png"
aws s3 cp "$LOCAL_DIR/dcf_valuation.xlsx"          "$S3_MODELS_DIR/dcf_valuation.xlsx"
aws s3 cp "$LOCAL_DIR/dcf_sensitivity_heatmap.png" "$S3_MODELS_DIR/dcf_sensitivity_heatmap.png"
aws s3 cp "$LOCAL_DIR/burn_rate_runway.csv"        "$S3_MODELS_DIR/burn_rate_runway.csv"

aws s3 ls "$S3_MODELS_DIR/"
python - <<'PY'
import pandas as pd
from pathlib import Path

year = "2025"
base = Path(f"data/processed/snowflake/10k/{year}")

# DCF base case + sensitivity long
xlsx = base / "dcf_valuation.xlsx"
dcf_base = pd.read_excel(xlsx, sheet_name="Base_Case")
dcf_base.to_csv(base / "dcf_base_case.csv", index=False)

sens = pd.read_excel(xlsx, sheet_name="Sensitivity")
sens = sens.rename(columns={sens.columns[0]: "wacc"})
sens_long = sens.melt(id_vars=["wacc"], var_name="terminal_growth", value_name="enterprise_value_musd")
sens_long["terminal_growth"] = sens_long["terminal_growth"].astype(float)
sens_long.to_csv(base / "dcf_sensitivity_long.csv", index=False)
PY
BASE="s3://$S3_BUCKET/tableau/snowflake/$YEAR"

aws s3 cp "data/processed/snowflake/10k/$YEAR/cleaned_financials.csv"   "$BASE/cleaned/cleaned_financials.csv"
aws s3 cp "data/processed/snowflake/10k/$YEAR/forecast_revenue.csv"     "$BASE/forecast/forecast_revenue.csv"
aws s3 cp "data/processed/snowflake/10k/$YEAR/forecast_metrics.csv"     "$BASE/forecast_metrics/forecast_metrics.csv"
aws s3 cp "data/processed/snowflake/10k/$YEAR/burn_rate_runway.csv"     "$BASE/burn/burn_rate_runway.csv"
aws s3 cp "data/processed/snowflake/10k/$YEAR/dcf_base_case.csv"        "$BASE/dcf_base/dcf_base_case.csv"
aws s3 cp "data/processed/snowflake/10k/$YEAR/dcf_sensitivity_long.csv" "$BASE/dcf_sensitivity/dcf_sensitivity_long.csv"

aws s3 ls "$BASE/" --recursive
SELECT 1;
SHOW TABLES IN snowflake_tableau;
CREATE OR REPLACE VIEW snowflake_tableau.v_financials AS
SELECT
  try_cast(substr(cast(period_end AS varchar), 1, 10) AS date) AS period_end,
  try_cast(fiscal_year AS integer) AS fiscal_year,
  fiscal_period,
  try_cast(revenue AS double) AS revenue,
  try_cast(netincome AS double) AS net_income,
  try_cast(operatingcashflow AS double) AS operating_cash_flow,
  try_cast(free_cash_flow AS double) AS free_cash_flow,
  try_cast(gross_margin_pct AS double) AS gross_margin_pct,
  try_cast(revenue_growth_yoy_pct AS double) AS revenue_growth_yoy_pct,
  try_cast(cashandequivalents AS double) AS cash_and_equivalents,
  try_cast(runway_months AS double) AS runway_months,
  CASE
    WHEN lower(cast(runway_risk_lt_12m AS varchar)) IN ('true','1','t','yes','y') THEN true
    ELSE false
  END AS runway_risk_lt_12m
FROM snowflake_tableau.tbl_2025_burn;
CREATE OR REPLACE VIEW snowflake_tableau.v_forecast AS
SELECT
  try_cast(substr(cast(period_end AS varchar), 1, 10) AS date) AS period_end,
  model,
  try_cast(forecast_revenue AS double) AS forecast_revenue,
  try_cast(lower_ci AS double) AS lower_ci,
  try_cast(upper_ci AS double) AS upper_ci,
  CASE
    WHEN lower(cast(is_best_model AS varchar)) IN ('true','1','t','yes','y') THEN true
    ELSE false
  END AS is_best_model
FROM snowflake_tableau.tbl_2025_forecast;
CREATE OR REPLACE VIEW snowflake_tableau.v_forecast_metrics AS
SELECT
  model,
  try_cast(mae AS double) AS mae,
  try_cast(rmse AS double) AS rmse,
  try_cast(mape_pct AS double) AS mape_pct,
  try_cast(smape_pct AS double) AS smape_pct,
  try_cast(holdout_points AS integer) AS holdout_points
FROM snowflake_tableau.tbl_2025_forecast_metrics;
CREATE OR REPLACE VIEW snowflake_tableau.v_dcf_base AS
SELECT
  try_cast(base_case_ev_musd AS double) AS base_case_ev_musd,
  try_cast(wacc AS double) AS wacc,
  try_cast(terminal_growth AS double) AS terminal_growth,
  try_cast(historical_points AS integer) AS historical_points
FROM snowflake_tableau.tbl_2025_dcf_base;
CREATE OR REPLACE VIEW snowflake_tableau.v_dcf_sensitivity AS
SELECT
  try_cast(wacc AS double) AS wacc,
  try_cast(terminal_growth AS double) AS terminal_growth,
  try_cast(enterprise_value_musd AS double) AS enterprise_value_musd
FROM snowflake_tableau.tbl_2025_dcf_sensitivity;
SELECT * FROM snowflake_tableau.v_forecast LIMIT 20;
SELECT * FROM snowflake_tableau.v_dcf_sensitivity LIMIT 20;
SELECT * FROM snowflake_tableau.v_financials;
SELECT * FROM snowflake_tableau.v_forecast;
SELECT * FROM snowflake_tableau.v_forecast_metrics;
SELECT * FROM snowflake_tableau.v_dcf_base;
SELECT * FROM snowflake_tableau.v_dcf_sensitivity;
