"""Tests for elec_data.collectors.gridstatus_collector.GridstatusCollector.

Unit tests use mocked gridstatus responses (no network needed).
Integration tests (marked @pytest.mark.integration) hit real APIs.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from elec_data.collectors.gridstatus_collector import (
    AESO_FUEL_MAP,
    IESO_FUEL_MAP,
    GridstatusCollector,
)
from elec_data.harmonize.schemas import validate_dataframe, SCHEMA_MAP


# ---- Helpers: mock gridstatus DataFrames ----


def _make_aeso_pool_price(n: int = 3) -> pd.DataFrame:
    """Create a fake AESO get_pool_price() return value."""
    start = pd.Timestamp("2024-06-01", tz="America/Edmonton")
    intervals = pd.date_range(start, periods=n, freq="h")
    return pd.DataFrame(
        {
            "Interval Start": intervals,
            "Interval End": intervals + pd.Timedelta(hours=1),
            "Pool Price": [50.0 + i for i in range(n)],
            "Rolling 30 Day Average Pool Price": [45.0] * n,
        }
    )


def _make_aeso_load(n: int = 3) -> pd.DataFrame:
    """Create a fake AESO get_load() return value."""
    start = pd.Timestamp("2024-06-01", tz="America/Edmonton")
    intervals = pd.date_range(start, periods=n, freq="h")
    return pd.DataFrame(
        {
            "Interval Start": intervals,
            "Interval End": intervals + pd.Timedelta(hours=1),
            "Load": [9000.0 + i * 100 for i in range(n)],
        }
    )


def _make_aeso_fuel_mix() -> pd.DataFrame:
    """Create a fake AESO get_fuel_mix() return value (wide format)."""
    t = pd.Timestamp("2024-06-01 12:00", tz="America/Edmonton")
    return pd.DataFrame(
        {
            "Time": [t],
            "Cogeneration": [1500.0],
            "Combined Cycle": [2000.0],
            "Gas Fired Steam": [300.0],
            "Simple Cycle": [200.0],
            "Hydro": [800.0],
            "Wind": [1200.0],
            "Solar": [600.0],
            "Energy Storage": [50.0],
            "Other": [100.0],
        }
    )


def _make_ieso_hoep(n: int = 3) -> pd.DataFrame:
    """Create a fake IESO get_hoep_historical_hourly() return value."""
    start = pd.Timestamp("2024-06-01", tz="America/Toronto")
    intervals = pd.date_range(start, periods=n, freq="h")
    return pd.DataFrame(
        {
            "Interval Start": intervals,
            "Interval End": intervals + pd.Timedelta(hours=1),
            "HOEP": [30.0 + i * 2 for i in range(n)],
            "Hour 1 Predispatch": [28.0] * n,
            "Hour 2 Predispatch": [29.0] * n,
            "Hour 3 Predispatch": [30.0] * n,
            "OR 10 Min Sync": [5.0] * n,
            "OR 10 Min non-sync": [3.0] * n,
            "OR 30 Min": [2.0] * n,
        }
    )


def _make_ieso_zonal_load(n: int = 3) -> pd.DataFrame:
    """Create a fake IESO get_load_zonal_hourly() return value."""
    start = pd.Timestamp("2024-06-01", tz="America/Toronto")
    intervals = pd.date_range(start, periods=n, freq="h")
    return pd.DataFrame(
        {
            "Interval Start": intervals,
            "Interval End": intervals + pd.Timedelta(hours=1),
            "Ontario Demand": [18000.0 + i * 200 for i in range(n)],
            "Northwest": [1000.0] * n,
            "Northeast": [800.0] * n,
            "Ottawa": [1200.0] * n,
            "East": [900.0] * n,
            "Toronto": [5000.0] * n,
            "Essa": [1500.0] * n,
            "Bruce": [2000.0] * n,
            "Southwest": [1800.0] * n,
            "Niagara": [1200.0] * n,
            "West": [1100.0] * n,
            "Zones Total": [16500.0] * n,
            "Diff": [1500.0] * n,
        }
    )


def _make_ieso_fuel_mix(n: int = 3) -> pd.DataFrame:
    """Create a fake IESO get_fuel_mix() return value (wide format)."""
    start = pd.Timestamp("2024-06-01", tz="America/Toronto")
    intervals = pd.date_range(start, periods=n, freq="h")
    return pd.DataFrame(
        {
            "Interval Start": intervals,
            "Interval End": intervals + pd.Timedelta(hours=1),
            "Biofuel": [100.0] * n,
            "Gas": [3000.0] * n,
            "Hydro": [5000.0] * n,
            "Nuclear": [9000.0] * n,
            "Solar": [200.0] * n,
            "Wind": [1500.0] * n,
            "Other": [50.0] * n,
        }
    )


# ---- Fixtures ----


@pytest.fixture()
def collector():
    """GridstatusCollector with default registry."""
    return GridstatusCollector()


# ---- Basic construction ----


class TestInit:
    def test_creates_instance(self, collector):
        assert isinstance(collector, GridstatusCollector)

    def test_supported_markets(self, collector):
        assert collector.supported_markets == ["AESO", "IESO"]

    def test_unsupported_market_prices(self, collector):
        with pytest.raises(ValueError, match="not supported"):
            collector.collect_prices("PJM", "2024-01-01", "2024-01-02")

    def test_unsupported_market_demand(self, collector):
        with pytest.raises(ValueError, match="not supported"):
            collector.collect_demand("FAKE", "2024-01-01", "2024-01-02")

    def test_unsupported_market_generation(self, collector):
        with pytest.raises(ValueError, match="not supported"):
            collector.collect_generation("ERCOT", "2024-01-01", "2024-01-02")


# ---- Fuel type mappings ----


class TestFuelMaps:
    def test_aeso_gas_types(self):
        for fuel in ["Cogeneration", "Combined Cycle", "Gas Fired Steam", "Simple Cycle"]:
            assert AESO_FUEL_MAP[fuel] == "gas"

    def test_aeso_renewables(self):
        assert AESO_FUEL_MAP["Wind"] == "wind"
        assert AESO_FUEL_MAP["Solar"] == "solar"
        assert AESO_FUEL_MAP["Hydro"] == "hydro"

    def test_aeso_storage(self):
        assert AESO_FUEL_MAP["Energy Storage"] == "storage"

    def test_aeso_other(self):
        assert AESO_FUEL_MAP["Other"] == "other"

    def test_ieso_nuclear(self):
        assert IESO_FUEL_MAP["Nuclear"] == "nuclear"

    def test_ieso_biofuel(self):
        assert IESO_FUEL_MAP["Biofuel"] == "biomass"

    def test_ieso_standard_fuels(self):
        assert IESO_FUEL_MAP["Gas"] == "gas"
        assert IESO_FUEL_MAP["Hydro"] == "hydro"
        assert IESO_FUEL_MAP["Wind"] == "wind"
        assert IESO_FUEL_MAP["Solar"] == "solar"


# ---- AESO price transformation ----


class TestAesoPrices:
    def test_columns_match_schema(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["prices"])
        assert errors == []

    def test_timestamps_are_utc(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_market_is_aeso(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        assert (result["market"] == "AESO").all()

    def test_currency_is_cad(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        assert (result["currency"] == "CAD").all()

    def test_price_type_is_pool(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        assert (result["price_type"] == "pool").all()

    def test_source(self, collector):
        raw = _make_aeso_pool_price()
        result = collector._transform_aeso_prices(raw)
        assert (result["source"] == "gridstatus_aeso").all()

    def test_price_values_preserved(self, collector):
        raw = _make_aeso_pool_price(3)
        result = collector._transform_aeso_prices(raw)
        assert list(result["price"]) == [50.0, 51.0, 52.0]

    def test_row_count(self, collector):
        raw = _make_aeso_pool_price(5)
        result = collector._transform_aeso_prices(raw)
        assert len(result) == 5

    def test_empty_input(self, collector):
        raw = pd.DataFrame(columns=["Interval Start", "Pool Price"])
        result = collector._transform_aeso_prices(raw)
        assert result.empty
        assert "timestamp_utc" in result.columns

    def test_sorted_by_timestamp(self, collector):
        raw = _make_aeso_pool_price(5)
        result = collector._transform_aeso_prices(raw)
        assert result["timestamp_utc"].is_monotonic_increasing


# ---- AESO demand transformation ----


class TestAesoDemand:
    def test_columns_match_schema(self, collector):
        raw = _make_aeso_load()
        result = collector._transform_aeso_demand(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["demand"])
        assert errors == []

    def test_timestamps_are_utc(self, collector):
        raw = _make_aeso_load()
        result = collector._transform_aeso_demand(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_demand_values_preserved(self, collector):
        raw = _make_aeso_load(3)
        result = collector._transform_aeso_demand(raw)
        assert list(result["demand_mw"]) == [9000.0, 9100.0, 9200.0]

    def test_demand_type_actual(self, collector):
        raw = _make_aeso_load()
        result = collector._transform_aeso_demand(raw)
        assert (result["demand_type"] == "actual").all()

    def test_empty_input(self, collector):
        raw = pd.DataFrame(columns=["Interval Start", "Load"])
        result = collector._transform_aeso_demand(raw)
        assert result.empty


# ---- AESO generation transformation ----


class TestAesoGeneration:
    def test_columns_match_schema(self, collector):
        raw = _make_aeso_fuel_mix()
        result = collector._transform_aeso_generation(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["generation"])
        assert errors == []

    def test_gas_aggregated(self, collector):
        """Cogeneration + Combined Cycle + Gas Fired Steam + Simple Cycle → gas."""
        raw = _make_aeso_fuel_mix()
        result = collector._transform_aeso_generation(raw)
        gas_row = result[result["fuel_type"] == "gas"]
        assert len(gas_row) == 1
        # 1500 + 2000 + 300 + 200 = 4000
        assert gas_row.iloc[0]["generation_mw"] == 4000.0

    def test_wind_solar_separate(self, collector):
        raw = _make_aeso_fuel_mix()
        result = collector._transform_aeso_generation(raw)
        assert result[result["fuel_type"] == "wind"].iloc[0]["generation_mw"] == 1200.0
        assert result[result["fuel_type"] == "solar"].iloc[0]["generation_mw"] == 600.0

    def test_fuel_types_correct(self, collector):
        raw = _make_aeso_fuel_mix()
        result = collector._transform_aeso_generation(raw)
        expected_fuels = {"gas", "hydro", "wind", "solar", "storage", "other"}
        assert set(result["fuel_type"]) == expected_fuels

    def test_timestamps_are_utc(self, collector):
        raw = _make_aeso_fuel_mix()
        result = collector._transform_aeso_generation(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_empty_input(self, collector):
        raw = pd.DataFrame(columns=["Time"])
        result = collector._transform_aeso_generation(raw)
        assert result.empty


# ---- IESO price transformation ----


class TestIesoPrices:
    def test_columns_match_schema(self, collector):
        raw = _make_ieso_hoep()
        result = collector._transform_ieso_prices(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["prices"])
        assert errors == []

    def test_timestamps_are_utc(self, collector):
        raw = _make_ieso_hoep()
        result = collector._transform_ieso_prices(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_currency_is_cad(self, collector):
        raw = _make_ieso_hoep()
        result = collector._transform_ieso_prices(raw)
        assert (result["currency"] == "CAD").all()

    def test_price_values_preserved(self, collector):
        raw = _make_ieso_hoep(3)
        result = collector._transform_ieso_prices(raw)
        assert list(result["price"]) == [30.0, 32.0, 34.0]

    def test_source(self, collector):
        raw = _make_ieso_hoep()
        result = collector._transform_ieso_prices(raw)
        assert (result["source"] == "gridstatus_ieso").all()


# ---- IESO demand transformation ----


class TestIesoDemand:
    def test_columns_match_schema(self, collector):
        raw = _make_ieso_zonal_load()
        result = collector._transform_ieso_demand(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["demand"])
        assert errors == []

    def test_uses_ontario_demand(self, collector):
        raw = _make_ieso_zonal_load(3)
        result = collector._transform_ieso_demand(raw)
        assert list(result["demand_mw"]) == [18000.0, 18200.0, 18400.0]

    def test_timestamps_are_utc(self, collector):
        raw = _make_ieso_zonal_load()
        result = collector._transform_ieso_demand(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"


# ---- IESO generation transformation ----


class TestIesoGeneration:
    def test_columns_match_schema(self, collector):
        raw = _make_ieso_fuel_mix()
        result = collector._transform_ieso_generation(raw)
        errors = validate_dataframe(result, SCHEMA_MAP["generation"])
        assert errors == []

    def test_fuel_types_correct(self, collector):
        raw = _make_ieso_fuel_mix()
        result = collector._transform_ieso_generation(raw)
        expected = {"gas", "hydro", "nuclear", "wind", "solar", "biomass", "other"}
        # Each timestamp has all fuel types, check distinct set
        assert set(result["fuel_type"]) == expected

    def test_nuclear_value(self, collector):
        raw = _make_ieso_fuel_mix(1)
        result = collector._transform_ieso_generation(raw)
        nuclear = result[result["fuel_type"] == "nuclear"]
        assert nuclear.iloc[0]["generation_mw"] == 9000.0

    def test_timestamps_are_utc(self, collector):
        raw = _make_ieso_fuel_mix()
        result = collector._transform_ieso_generation(raw)
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_row_count(self, collector):
        """3 timestamps x 7 fuel types = 21 rows."""
        raw = _make_ieso_fuel_mix(3)
        result = collector._transform_ieso_generation(raw)
        assert len(result) == 21


# ---- Lazy ISO initialization ----


class TestLazyInit:
    def test_no_aeso_key_raises_on_call(self):
        """AESO should only fail when actually called, not at construction."""
        # Construction should succeed (lazy init)
        c = GridstatusCollector()
        assert "AESO" in c.supported_markets

    def test_ieso_no_key_needed(self, collector):
        """IESO should initialize without an API key via mocked _get_iso."""
        mock_iso = MagicMock()
        mock_iso.get_hoep_historical_hourly.return_value = _make_ieso_hoep()

        with patch.object(collector, "_get_iso", return_value=mock_iso):
            result = collector.collect_prices("IESO", "2024-06-01", "2024-06-02")
        assert len(result) == 3


# ---- Full collect_* methods with mocks ----


class TestCollectWithMocks:
    def test_collect_ieso_prices(self):
        mock_iso = MagicMock()
        mock_iso.get_hoep_historical_hourly.return_value = _make_ieso_hoep(5)

        collector = GridstatusCollector()
        with patch.object(collector, "_get_iso", return_value=mock_iso):
            result = collector.collect_prices("IESO", "2024-06-01", "2024-06-02")
        assert len(result) == 5
        assert (result["market"] == "IESO").all()

    def test_collect_ieso_demand(self):
        mock_iso = MagicMock()
        mock_iso.get_load_zonal_hourly.return_value = _make_ieso_zonal_load(4)

        collector = GridstatusCollector()
        with patch.object(collector, "_get_iso", return_value=mock_iso):
            result = collector.collect_demand("IESO", "2024-06-01", "2024-06-02")
        assert len(result) == 4
        assert (result["market"] == "IESO").all()

    def test_collect_ieso_generation(self):
        mock_iso = MagicMock()
        mock_iso.get_fuel_mix.return_value = _make_ieso_fuel_mix(2)

        collector = GridstatusCollector()
        with patch.object(collector, "_get_iso", return_value=mock_iso):
            result = collector.collect_generation("IESO", "2024-06-01", "2024-06-02")
        # 2 timestamps x 7 fuels = 14
        assert len(result) == 14
        assert (result["market"] == "IESO").all()


# ---- Integration tests (require network + API keys) ----


@pytest.mark.integration
class TestIntegrationIeso:
    """Live IESO tests. No API key needed."""

    def test_ieso_prices_live(self):
        collector = GridstatusCollector()
        result = collector.collect_prices("IESO", "2024-01-01", "2024-01-02")
        assert len(result) > 0
        errors = validate_dataframe(result, SCHEMA_MAP["prices"])
        assert errors == []
        assert (result["market"] == "IESO").all()
        assert str(result["timestamp_utc"].dt.tz) == "UTC"

    def test_ieso_demand_live(self):
        collector = GridstatusCollector()
        result = collector.collect_demand("IESO", "2024-01-01", "2024-01-02")
        assert len(result) > 0
        errors = validate_dataframe(result, SCHEMA_MAP["demand"])
        assert errors == []

    def test_ieso_generation_live(self):
        collector = GridstatusCollector()
        result = collector.collect_generation("IESO", "2024-01-01", "2024-01-02")
        assert len(result) > 0
        errors = validate_dataframe(result, SCHEMA_MAP["generation"])
        assert errors == []


@pytest.mark.integration
class TestIntegrationAeso:
    """Live AESO tests. Require AESO_API_KEY environment variable."""

    def test_aeso_prices_live(self):
        collector = GridstatusCollector()
        result = collector.collect_prices("AESO", "2024-06-01", "2024-06-02")
        assert len(result) > 0
        errors = validate_dataframe(result, SCHEMA_MAP["prices"])
        assert errors == []
        assert (result["market"] == "AESO").all()

    def test_aeso_demand_live(self):
        collector = GridstatusCollector()
        result = collector.collect_demand("AESO", "2024-06-01", "2024-06-02")
        assert len(result) > 0
        errors = validate_dataframe(result, SCHEMA_MAP["demand"])
        assert errors == []
