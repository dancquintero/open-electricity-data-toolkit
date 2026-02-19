"""Shared pytest fixtures for elec_data tests."""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def sample_price_df() -> pd.DataFrame:
    """Minimal valid DataFrame matching PriceRecord schema."""
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime(
            ["2024-01-01 00:00", "2024-01-01 01:00", "2024-01-01 02:00"],
            utc=True,
        ),
        "market": ["AESO", "AESO", "AESO"],
        "price": [45.50, 52.30, 48.10],
        "currency": ["CAD", "CAD", "CAD"],
        "price_type": ["pool", "pool", "pool"],
        "resolution_minutes": [60, 60, 60],
        "source": ["gridstatus_aeso", "gridstatus_aeso", "gridstatus_aeso"],
    })


@pytest.fixture
def sample_demand_df() -> pd.DataFrame:
    """Minimal valid DataFrame matching DemandRecord schema."""
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime(
            ["2024-01-01 00:00", "2024-01-01 01:00"],
            utc=True,
        ),
        "market": ["AESO", "AESO"],
        "demand_mw": [9500.0, 9600.0],
        "demand_type": ["actual", "actual"],
        "resolution_minutes": [60, 60],
        "source": ["gridstatus_aeso", "gridstatus_aeso"],
    })


@pytest.fixture
def sample_generation_df() -> pd.DataFrame:
    """Minimal valid DataFrame matching GenerationRecord schema."""
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime(
            ["2024-01-01 00:00", "2024-01-01 00:00", "2024-01-01 01:00", "2024-01-01 01:00"],
            utc=True,
        ),
        "market": ["AESO", "AESO", "AESO", "AESO"],
        "fuel_type": ["gas", "wind", "gas", "wind"],
        "generation_mw": [5000.0, 1200.0, 5200.0, 1100.0],
        "resolution_minutes": [60, 60, 60, 60],
        "source": ["gridstatus_aeso", "gridstatus_aeso", "gridstatus_aeso", "gridstatus_aeso"],
    })


@pytest.fixture
def sample_flow_df() -> pd.DataFrame:
    """Minimal valid DataFrame matching FlowRecord schema."""
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime(
            ["2024-01-01 00:00", "2024-01-01 01:00"],
            utc=True,
        ),
        "from_market": ["AESO", "AESO"],
        "to_market": ["BC", "BC"],
        "flow_mw": [200.0, 180.0],
        "resolution_minutes": [60, 60],
        "source": ["gridstatus_aeso", "gridstatus_aeso"],
    })
