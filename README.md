# snowflake-10k-pipeline

Production-grade Python + AWS pipeline for Snowflake (SNOW) 10-K ingestion, financial statement normalization, KPI engineering, forecasting, DCF valuation, burn/runway analysis, and Tableau reporting.

## Architecture Diagram Description

The architecture follows a staged data platform pattern:
1. SEC EDGAR ingestion layer calls `data.sec.gov` submissions and companyfacts APIs to collect the latest and historical 10-K related financial facts.
2. Raw statement extracts are stored locally and in S3 under `/raw/snowflake/10k/<year>/`.
3. Cleaning layer standardizes schemas, parses periods, normalizes values to USD millions, imputes missing values, applies accounting checks, and creates derived KPI features.
4. Processed dataset is saved to S3 under `/processed/snowflake/10k/<year>/cleaned_financials.csv`.
5. Modeling layer produces revenue forecasts (Linear Regression, ARIMA, Prophet), DCF sensitivity outputs, and burn/runway metrics to `/models/snowflake/<year>/`.
6. AWS Lambda orchestrates the full flow on EventBridge schedule; CloudWatch monitors execution; SNS notifies on success/failure; SQS DLQ captures failed Lambda invocations.
7. Tableau consumes processed and model outputs from local extract, Athena/S3, or exported CSVs.


## Repository Layout

```text
snowflake-10k-pipeline/
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
│   ├── 01_ingestion.ipynb
│   ├── 02_cleaning.ipynb
│   ├── 03_modeling.ipynb
│   └── 04_eda.ipynb
├── src/
│   ├── ingestion/
│   │   └── sec_downloader.py
│   ├── cleaning/
│   │   └── cleaner.py
│   ├── models/
│   │   ├── revenue_forecast.py
│   │   ├── dcf_model.py
│   │   └── burn_rate.py
│   └── cloud/
│       ├── s3_uploader.py
│       └── lambda_handler.py
├── tableau/
│   └── snowflake_dashboard.twbx
├── infrastructure/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── tests/
│   └── test_models.py
├── requirements.txt
├── .env.example
└── README.md
```

## Setup Instructions

1. Create and activate a virtual environment:
```bash
python3.10 -m venv .venv
source .venv/bin/activate
```
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with AWS credentials/role-based settings and SEC user-agent
```
4. Load env vars:
```bash
set -a && source .env && set +a
```

## Run Pipeline Manually

1. Ingestion:
```bash
python -m src.ingestion.sec_downloader
```
2. Cleaning:
```bash
python -m src.cleaning.cleaner --year 2025
```
3. Models:
```bash
python -m src.models.revenue_forecast --year 2025 --periods 4
python -m src.models.dcf_model --year 2025 --wacc 0.10 --terminal-growth 0.03
python -m src.models.burn_rate --year 2025
```
4. Full local orchestration:
```bash
python -m src.pipeline
```

## Automated Run (AWS)

1. Package Lambda source (zip including `src/` and dependencies layer as needed).
2. Initialize Terraform:
```bash
cd infrastructure
terraform init
```
3. Apply infrastructure:
```bash
terraform apply \
  -var="s3_bucket_name=<bucket>" \
  -var="lambda_package_path=../dist/lambda.zip" \
  -var="notification_email=<email>" \
  -var="sec_user_agent=<name email>"
```
4. EventBridge rule triggers yearly by default (`rate(1 year)`), or set a custom schedule with `schedule_expression`.

## Tableau Dashboard Build Guide

Live dashboard: [View the Snowflake Financial Dashboard](https://public.tableau.com/app/profile/shravan.kumaar/viz/Snowflake_Dashboard/Dashboard1)

### Data Connection Options
1. Direct file mode:
   - Use local `data/processed/snowflake/10k/<year>/cleaned_financials.csv`
   - Use model outputs from `data/processed/snowflake/10k/<year>/`
2. Athena mode:
   - Use Glue Crawler + Data Catalog tables on S3 processed/model prefixes.
   - Connect Tableau to Amazon Athena and query cataloged datasets.
3. Publish mode:
   - Tableau Public: publish workbook and data extract.
   - Tableau Server/Cloud: publish with scheduled refresh using Athena/Bridge.

### Required Dashboards
1. Financial Overview: KPI cards, yearly revenue bars with YoY annotation, gross margin trend.
2. Profitability Deep-Dive: revenue mix stacked bars, waterfall, growth vs margin scatter.
3. Cash Flow & Liquidity: operating/investing/financing trends, runway gauge, cash table.
4. Revenue Forecast: model toggle (Linear/ARIMA/Prophet), confidence band, assumptions annotation.
5. DCF Sensitivity Heatmap: WACC vs terminal growth matrix with highlighted base-case.

### Visual Standards
- Palette: navy, teal, amber.
- Time axis: fiscal year/quarter everywhere.
- Controls: WACC, terminal growth, date range filters.

## Model Assumptions and Limitations

1. SEC extraction uses XBRL tags from `companyfacts`; custom company tags or taxonomy shifts may require tag map updates.
2. Missing values are forward-filled; this can smooth real volatility.
3. YoY anomaly threshold is a fixed absolute 200% rule.
4. DCF uses historical FCF CAGR projection and a constant WACC/terminal growth framework.
5. Forecast quality depends on available quarterly history; Prophet is optional and may be skipped if not installed.
6. Net income reconciliation uses heuristic thresholding rather than full statement-level line-item reconstruction.

## Quality and Testing

Run unit tests:
```bash
pytest -q
```

Linting:
```bash
ruff check src tests
```

## Security and Operational Notes

1. Do not hardcode credentials. Use IAM roles in AWS runtime and env vars locally.
2. SEC API requests must include a valid `SEC_USER_AGENT`.
3. S3 bucket versioning and lifecycle (Glacier after 90 days) are provisioned in Terraform.
4. Lambda failures route to SQS DLQ; CloudWatch alarms notify via SNS.
