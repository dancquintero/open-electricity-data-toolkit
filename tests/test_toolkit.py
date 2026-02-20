"""Tests for elec_data.toolkit.Toolkit.

Unit tests use a mock collector so no network or API keys are needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from elec_data.toolkit import Toolkit


# ---- Helpers: build mock collector DataFrames ----


def _mock_prices(market: str = "AESO", n: int = 24) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame({
        "timestamp_utc": ts,
        "market": market,
        "price": [50.0 + i for i in range(n)],
        "currency": "CAD",
        "price_type": "pool",
        "resolution_minutes": 60,
        "source": f"gridstatus_{market.lower()}",
    })


def _mock_demand(market: str = "AESO", n: int = 24) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame({
        "timestamp_utc": ts,
        "market": market,
        "demand_mw": [9000.0 + i * 10 for i in range(n)],
        "demand_type": "actual",
        "resolution_minutes": 60,
        "source": f"gridstatus_{market.lower()}",
    })


def _mock_generation(market: str = "AESO", n: int = 4) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    rows = []
    for t in ts:
        for fuel, mw in [("gas", 5000.0), ("wind", 1200.0), ("solar", 600.0)]:
            rows.append({
                "timestamp_utc": t,
                "market": market,
                "fuel_type": fuel,
                "generation_mw": mw,
                "resolution_minutes": 60,
                "source": f"gridstatus_{market.lower()}",
            })
    return pd.DataFrame(rows)


def _make_mock_collector():
    """Build a mock collector that returns canned data."""
    mock = MagicMock()
    mock.supported_markets = ["AESO", "IESO"]
    mock.collect_prices.side_effect = lambda m, s, e: _mock_prices(m)
    mock.collect_demand.side_effect = lambda m, s, e: _mock_demand(m)
    mock.collect_generation.side_effect = lambda m, s, e: _mock_generation(m)
    return mock


@pytest.fixture()
def tk(tmp_path):
    """Toolkit with a mock collector pointed at a temp directory."""
    toolkit = Toolkit(data_dir=tmp_path)
    toolkit._collectors = [_make_mock_collector()]
    return toolkit


# ---- Init ----


class TestInit:
    def test_creates_data_dir(self, tmp_path):
        d = tmp_path / "new_data"
        Toolkit(data_dir=d)
        assert d.exists()

    def test_has_store_and_log(self, tk):
        assert tk._store is not None
        assert tk._log is not None
        assert tk._registry is not None


# ---- collect() ----


class TestCollect:
    def test_collect_stores_data(self, tk):
        tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-02-01")
        result = tk._store.read("AESO", "prices", "2024-01-01", "2024-02-01")
        assert len(result) > 0

    def test_collect_logs_success(self, tk):
        tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-02-01")
        log_status = tk._log.status()
        assert len(log_status) > 0
        assert "AESO" in log_status["market"].values

    def test_collect_multiple_data_types(self, tk):
        tk.collect(["AESO"], ["prices", "demand"], "2024-01-01", "2024-02-01")
        assert len(tk._store.read("AESO", "prices", "2024-01-01", "2024-02-01")) > 0
        assert len(tk._store.read("AESO", "demand", "2024-01-01", "2024-02-01")) > 0

    def test_collect_multiple_markets(self, tk):
        tk.collect(["AESO", "IESO"], ["prices"], "2024-01-01", "2024-02-01")
        assert len(tk._store.read("AESO", "prices", "2024-01-01", "2024-02-01")) > 0
        assert len(tk._store.read("IESO", "prices", "2024-01-01", "2024-02-01")) > 0

    def test_collect_unsupported_market_logs_error(self, tk):
        tk.collect(["FAKE_MARKET"], ["prices"], "2024-01-01", "2024-02-01")
        log = tk._log.status()
        # Should not crash; error is logged but no data stored
        stored = tk._store.read("FAKE_MARKET", "prices", "2024-01-01", "2024-02-01")
        assert stored.empty

    def test_collect_chunks_by_month(self, tk):
        tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-03-01")
        collector = tk._collectors[0]
        assert collector.collect_prices.call_count == 2  # Jan and Feb


# ---- get_prices() ----


class TestGetPrices:
    def test_auto_fetches_when_empty(self, tk):
        result = tk.get_prices(["AESO"], "2024-01-01", "2024-02-01")
        assert len(result) > 0
        assert "timestamp_utc" in result.columns
        assert "price" in result.columns

    def test_returns_from_store_if_present(self, tk):
        tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-02-01")
        collector = tk._collectors[0]
        collector.collect_prices.reset_mock()

        result = tk.get_prices(["AESO"], "2024-01-01", "2024-02-01")
        assert len(result) > 0
        collector.collect_prices.assert_not_called()

    def test_utc_timestamps(self, tk):
        result = tk.get_prices(["AESO"], "2024-01-01", "2024-02-01")
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_sorted_by_timestamp(self, tk):
        result = tk.get_prices(["AESO"], "2024-01-01", "2024-02-01")
        assert result["timestamp_utc"].is_monotonic_increasing

    def test_empty_markets_returns_empty_df(self, tk):
        result = tk.get_prices([], "2024-01-01", "2024-02-01")
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "timestamp_utc" in result.columns

    def test_multi_market(self, tk):
        result = tk.get_prices(["AESO", "IESO"], "2024-01-01", "2024-02-01")
        markets = set(result["market"].unique())
        assert "AESO" in markets
        assert "IESO" in markets


# ---- get_demand() ----


class TestGetDemand:
    def test_returns_demand_data(self, tk):
        result = tk.get_demand(["AESO"], "2024-01-01", "2024-02-01")
        assert len(result) > 0
        assert "demand_mw" in result.columns

    def test_utc_timestamps(self, tk):
        result = tk.get_demand(["AESO"], "2024-01-01", "2024-02-01")
        assert str(result["timestamp_utc"].dt.tz) == "UTC"


# ---- get_generation() ----


class TestGetGeneration:
    def test_returns_generation_data(self, tk):
        result = tk.get_generation(["AESO"], "2024-01-01", "2024-02-01")
        assert len(result) > 0
        assert "fuel_type" in result.columns
        assert "generation_mw" in result.columns

    def test_fuel_type_filter(self, tk):
        result = tk.get_generation(
            ["AESO"], "2024-01-01", "2024-02-01", fuel_types=["wind"]
        )
        assert len(result) > 0
        assert set(result["fuel_type"].unique()) == {"wind"}

    def test_fuel_type_filter_multiple(self, tk):
        result = tk.get_generation(
            ["AESO"], "2024-01-01", "2024-02-01", fuel_types=["wind", "solar"]
        )
        assert set(result["fuel_type"].unique()).issubset({"wind", "solar"})


# ---- status() ----


class TestStatus:
    def test_empty_store(self, tk):
        result = tk.status()
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert "market" in result.columns

    def test_shows_collected_data(self, tk):
        tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-02-01")
        result = tk.status()
        assert len(result) > 0
        assert "AESO" in result["market"].values
        assert "prices" in result["data_type"].values

    def test_shows_multiple_types(self, tk):
        tk.collect(["AESO"], ["prices", "demand"], "2024-01-01", "2024-02-01")
        result = tk.status()
        assert len(result) >= 2
        data_types = set(result["data_type"].values)
        assert "prices" in data_types
        assert "demand" in data_types


# ---- _monthly_chunks() ----


class TestMonthlyChunks:
    def test_single_month(self):
        chunks = Toolkit._monthly_chunks("2024-01-01", "2024-02-01")
        assert chunks == [("2024-01-01", "2024-02-01")]

    def test_two_months(self):
        chunks = Toolkit._monthly_chunks("2024-01-01", "2024-03-01")
        assert chunks == [("2024-01-01", "2024-02-01"), ("2024-02-01", "2024-03-01")]

    def test_partial_month(self):
        chunks = Toolkit._monthly_chunks("2024-01-15", "2024-02-10")
        assert len(chunks) == 2
        assert chunks[0] == ("2024-01-15", "2024-02-01")
        assert chunks[1] == ("2024-02-01", "2024-02-10")

    def test_same_month_range(self):
        chunks = Toolkit._monthly_chunks("2024-01-01", "2024-01-15")
        assert len(chunks) == 1
        assert chunks[0] == ("2024-01-01", "2024-01-15")


# ---- _get_collector() ----


class TestGetCollector:
    def test_finds_supported_market(self, tk):
        collector = tk._get_collector("AESO")
        assert collector is not None

    def test_raises_for_unsupported(self, tk):
        with pytest.raises(ValueError, match="not supported"):
            tk._get_collector("FAKE")

    def test_error_message_lists_supported(self, tk):
        with pytest.raises(ValueError, match="AESO"):
            tk._get_collector("UNKNOWN")
