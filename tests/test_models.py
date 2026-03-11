"""Unit tests for financial model calculations."""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.models.burn_rate import compute_burn_and_runway
from src.models.dcf_model import cagr, dcf_value, project_fcf



def test_cagr_basic() -> None:
    result = cagr(100.0, 121.0, 2)
    assert round(result, 4) == 0.1



def test_project_fcf_has_five_years() -> None:
    history = pd.Series([100.0, 110.0, 121.0], dtype=float)
    projected = project_fcf(history, years=5)
    assert len(projected) == 5
    assert projected.iloc[0] > 0



def test_dcf_value_positive() -> None:
    projected = pd.Series([100.0, 110.0, 120.0, 130.0, 140.0], dtype=float)
    ev = dcf_value(projected, wacc=0.1, terminal_growth=0.03)
    assert ev > 0



def test_compute_burn_and_runway_flags() -> None:
    df = pd.DataFrame(
        {
            "period_end": pd.to_datetime(["2024-01-31", "2024-04-30"]),
            "OperatingCashFlow": [-90.0, -30.0],
            "CashAndEquivalents": [180.0, 60.0],
        }
    )
    out = compute_burn_and_runway(df)

    assert np.isclose(out.loc[0, "quarterly_cash_burn"], 90.0)
    assert np.isclose(out.loc[0, "runway_months"], 6.0)
    assert bool(out.loc[0, "runway_risk_lt_12m"])
