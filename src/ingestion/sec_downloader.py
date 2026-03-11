"""Download and extract Snowflake 10-K financial data from SEC EDGAR."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.cloud.s3_uploader import S3Uploader
from src.common.config import load_config
from src.common.logging_utils import setup_logger

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

STATEMENT_TAGS = {
    "income_statement": {
        "Revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"],
        "COGS": ["CostOfRevenue", "CostOfGoodsAndServicesSold"],
        "OperatingExpenses": ["OperatingExpenses"],
        "NetIncome": ["NetIncomeLoss"],
        "OperatingLoss": ["OperatingIncomeLoss"],
    },
    "balance_sheet": {
        "TotalAssets": ["Assets"],
        "CurrentAssets": ["AssetsCurrent"],
        "TotalLiabilities": ["Liabilities"],
        "CurrentLiabilities": ["LiabilitiesCurrent"],
        "StockholdersEquity": ["StockholdersEquity"],
        "CashAndEquivalents": ["CashAndCashEquivalentsAtCarryingValue"],
        "TotalDebt": ["LongTermDebt", "LongTermDebtAndFinanceLeaseObligations"],
    },
    "cash_flow": {
        "OperatingCashFlow": ["NetCashProvidedByUsedInOperatingActivities"],
        "InvestingCashFlow": ["NetCashProvidedByUsedInInvestingActivities"],
        "FinancingCashFlow": ["NetCashProvidedByUsedInFinancingActivities"],
        "CapEx": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    },
}


@dataclass
class FilingMetadata:
    """Metadata for a single SEC filing."""

    accession_number: str
    filing_date: str
    report_date: str
    primary_document: str
    form: str


class Sec10KIngestor:
    """Extract Snowflake 10-K data from SEC EDGAR."""

    def __init__(self, cik: str, user_agent: str, output_dir: str = "data/raw") -> None:
        self.cik = cik.zfill(10)
        self.output_dir = Path(output_dir)
        self.session = self._build_session(user_agent)
        self.logger = setup_logger("sec_ingestion")

    @staticmethod
    def _build_session(user_agent: str) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )
        return session

    def _get_json(self, url: str) -> dict[str, Any]:
        self.logger.info("Fetching URL: %s", url)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_10k_filings(self) -> list[FilingMetadata]:
        """Collect Snowflake 10-K filings from SEC submissions endpoint.

        Returns:
            list[FilingMetadata]: 10-K filings sorted by filing date descending.
        """
        submissions = self._get_json(SUBMISSIONS_URL.format(cik=self.cik))
        filings_frames: list[pd.DataFrame] = []

        recent = pd.DataFrame(submissions.get("filings", {}).get("recent", {}))
        if not recent.empty:
            filings_frames.append(recent)

        for extra in submissions.get("filings", {}).get("files", []):
            name = extra.get("name")
            if not name:
                continue
            url = f"https://data.sec.gov/submissions/{name}"
            try:
                extra_json = self._get_json(url)
                filings_frames.append(pd.DataFrame(extra_json))
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Skipping paginated file %s: %s", name, exc)

        if not filings_frames:
            raise RuntimeError("No filings found in submissions payload")

        filings = pd.concat(filings_frames, ignore_index=True)
        tenk = filings[filings["form"] == "10-K"].copy()
        if tenk.empty:
            raise RuntimeError("No 10-K filings found")

        tenk["filingDate"] = pd.to_datetime(tenk["filingDate"], errors="coerce")
        tenk = tenk.sort_values("filingDate", ascending=False)

        output = [
            FilingMetadata(
                accession_number=row["accessionNumber"],
                filing_date=str(row["filingDate"].date()) if pd.notna(row["filingDate"]) else "",
                report_date=str(row.get("reportDate", "")),
                primary_document=row.get("primaryDocument", ""),
                form=row["form"],
            )
            for _, row in tenk.iterrows()
        ]
        self.logger.info("Found %s 10-K filings", len(output))
        return output

    def get_latest_10k_metadata(self) -> FilingMetadata:
        filings = self.get_10k_filings()
        return filings[0]

    def _extract_series(self, facts: dict[str, Any], tags: list[str]) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        us_gaap = facts.get("facts", {}).get("us-gaap", {})

        for tag in tags:
            tag_payload = us_gaap.get(tag)
            if not tag_payload:
                continue

            units = tag_payload.get("units", {})
            for unit, points in units.items():
                for point in points:
                    if point.get("form") not in {"10-K", "10-Q"}:
                        continue
                    records.append(
                        {
                            "source_tag": tag,
                            "unit": unit,
                            "value": point.get("val"),
                            "end_date": point.get("end"),
                            "fiscal_year": point.get("fy"),
                            "fiscal_period": point.get("fp"),
                            "filed": point.get("filed"),
                            "accession_no": point.get("accn"),
                            "form": point.get("form"),
                        }
                    )

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["filed"] = pd.to_datetime(df["filed"], errors="coerce")
        df = df.sort_values(["end_date", "filed"]).drop_duplicates(
            subset=["end_date", "fiscal_year", "fiscal_period", "source_tag"], keep="last"
        )
        return df

    def extract_financial_statements(self) -> dict[str, pd.DataFrame]:
        """Extract statement-level time series from XBRL companyfacts.

        Returns:
            dict[str, pd.DataFrame]: Statement dataframes.
        """
        facts = self._get_json(COMPANY_FACTS_URL.format(cik=self.cik))
        statements: dict[str, pd.DataFrame] = {}

        for statement_name, metric_tag_map in STATEMENT_TAGS.items():
            frames: list[pd.DataFrame] = []
            for metric_name, tags in metric_tag_map.items():
                metric_df = self._extract_series(facts, tags)
                if metric_df.empty:
                    self.logger.warning("No data found for %s in %s", metric_name, statement_name)
                    continue
                metric_df["metric"] = metric_name
                frames.append(metric_df)

            if frames:
                combined = pd.concat(frames, ignore_index=True)
                combined["end_date"] = pd.to_datetime(combined["end_date"], errors="coerce")
                combined = combined.sort_values(["end_date", "metric"])
                statements[statement_name] = combined
            else:
                statements[statement_name] = pd.DataFrame(
                    columns=[
                        "source_tag",
                        "unit",
                        "value",
                        "end_date",
                        "fiscal_year",
                        "fiscal_period",
                        "filed",
                        "accession_no",
                        "form",
                        "metric",
                    ]
                )
        return statements

    def save_statement_csvs(self, statements: dict[str, pd.DataFrame], year: str) -> dict[str, str]:
        """Persist extracted statements to local CSVs.

        Args:
            statements: Mapping of statement name to dataframe.
            year: Year partition for output folder.

        Returns:
            dict[str, str]: Local file paths keyed by statement.
        """
        base_dir = self.output_dir / "snowflake" / "10k" / year
        base_dir.mkdir(parents=True, exist_ok=True)

        mapping = {
            "income_statement": "income_statement.csv",
            "balance_sheet": "balance_sheet.csv",
            "cash_flow": "cash_flow.csv",
        }

        output: dict[str, str] = {}
        for statement_name, filename in mapping.items():
            path = base_dir / filename
            df = statements.get(statement_name, pd.DataFrame())
            df.to_csv(path, index=False)
            self.logger.info("Saved %s rows to %s", len(df), path)
            output[statement_name] = str(path)
        return output

    def get_latest_filing_html(self, filing: FilingMetadata) -> str:
        """Download latest 10-K HTML filing document for archival/debugging."""
        cik_int = str(int(self.cik))
        accession_no_dashless = filing.accession_number.replace("-", "")
        doc_url = (
            f"{ARCHIVES_BASE}/{cik_int}/{accession_no_dashless}/{filing.primary_document}"
        )
        self.logger.info("Downloading filing HTML: %s", doc_url)
        response = self.session.get(doc_url, timeout=30)
        response.raise_for_status()
        return response.text


def run_ingestion() -> dict[str, str]:
    """Main ingestion entrypoint.

    Returns:
        dict[str, str]: Local paths to extracted CSV files.
    """
    config = load_config()
    logger = setup_logger("ingestion_main", "data/raw/ingestion.log")
    ingestor = Sec10KIngestor(config.company_cik, config.sec_user_agent, output_dir="data/raw")

    logger.info("Starting ingestion for CIK=%s ticker=%s", config.company_cik, config.company_ticker)
    latest_filing = ingestor.get_latest_10k_metadata()
    logger.info("Latest 10-K filing date: %s", latest_filing.filing_date)

    statements = ingestor.extract_financial_statements()
    year = latest_filing.report_date[:4] if latest_filing.report_date else latest_filing.filing_date[:4]
    paths = ingestor.save_statement_csvs(statements, year=year)

    html_path = Path("data/raw/snowflake/10k") / year / "latest_filing.html"
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_content = ingestor.get_latest_filing_html(latest_filing)
    html_path.write_text(html_content, encoding="utf-8")
    logger.info("Saved filing html to %s", html_path)

    s3_uploader = S3Uploader(config.aws_region)
    for statement_name, local_path in paths.items():
        key = f"{config.raw_prefix}/{year}/{Path(local_path).name}"
        s3_uploader.upload_file(local_path, config.s3_bucket, key)
        logger.info("Uploaded %s to s3://%s/%s", statement_name, config.s3_bucket, key)

    html_key = f"{config.raw_prefix}/{year}/latest_filing.html"
    s3_uploader.upload_file(str(html_path), config.s3_bucket, html_key)
    logger.info("Uploaded raw html to s3://%s/%s", config.s3_bucket, html_key)

    return paths


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Download Snowflake 10-K financial statements from SEC")
    parser.add_argument("--print-only", action="store_true", help="Only print local output file paths")
    args = parser.parse_args()

    paths = run_ingestion()
    if args.print_only:
        print(json.dumps(paths, indent=2))


if __name__ == "__main__":
    _cli()
