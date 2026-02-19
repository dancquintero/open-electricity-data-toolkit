"""Tests for elec_data.registry.markets.MarketRegistry."""

from __future__ import annotations

import json

import pytest

from elec_data.registry.markets import MarketRegistry


# ---- Fixtures ----


@pytest.fixture()
def registry():
    """Default registry loaded from the bundled JSON."""
    return MarketRegistry()


@pytest.fixture()
def custom_registry(tmp_path):
    """Registry loaded from a minimal custom JSON file."""
    data = {
        "TEST": {
            "full_name": "Test Market",
            "country": "XX",
            "timezone": "UTC",
            "currency": "USD",
            "native_price_resolution_minutes": 60,
            "native_demand_resolution_minutes": 30,
            "native_generation_resolution_minutes": 15,
        }
    }
    path = tmp_path / "test_registry.json"
    path.write_text(json.dumps(data))
    return MarketRegistry(registry_path=path)


# ---- Init ----


class TestInit:
    def test_loads_bundled_registry(self, registry):
        assert len(registry.list_markets()) > 0

    def test_custom_path(self, custom_registry):
        assert custom_registry.list_markets() == ["TEST"]

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            MarketRegistry(registry_path=tmp_path / "nonexistent.json")


# ---- list_markets ----


class TestListMarkets:
    def test_returns_sorted(self, registry):
        markets = registry.list_markets()
        assert markets == sorted(markets)

    def test_contains_expected_markets(self, registry):
        markets = registry.list_markets()
        for m in ["AESO", "IESO", "DE_LU", "GB", "ES"]:
            assert m in markets

    def test_count(self, registry):
        assert len(registry.list_markets()) == 5


# ---- get ----


class TestGet:
    def test_returns_dict(self, registry):
        result = registry.get("AESO")
        assert isinstance(result, dict)

    def test_contains_required_fields(self, registry):
        result = registry.get("AESO")
        for field in ["full_name", "timezone", "currency"]:
            assert field in result

    def test_unknown_market_raises(self, registry):
        with pytest.raises(KeyError, match="Unknown market"):
            registry.get("FAKE")

    def test_returns_copy(self, registry):
        """Modifying the returned dict should not affect the registry."""
        result = registry.get("AESO")
        result["timezone"] = "MODIFIED"
        assert registry.get("AESO")["timezone"] == "America/Edmonton"


# ---- get_timezone ----


class TestGetTimezone:
    def test_aeso(self, registry):
        assert registry.get_timezone("AESO") == "America/Edmonton"

    def test_ieso(self, registry):
        assert registry.get_timezone("IESO") == "America/Toronto"

    def test_de_lu(self, registry):
        assert registry.get_timezone("DE_LU") == "Europe/Berlin"

    def test_gb(self, registry):
        assert registry.get_timezone("GB") == "Europe/London"

    def test_es(self, registry):
        assert registry.get_timezone("ES") == "Europe/Madrid"

    def test_unknown_raises(self, registry):
        with pytest.raises(KeyError):
            registry.get_timezone("FAKE")


# ---- get_currency ----


class TestGetCurrency:
    def test_cad_markets(self, registry):
        assert registry.get_currency("AESO") == "CAD"
        assert registry.get_currency("IESO") == "CAD"

    def test_eur_markets(self, registry):
        assert registry.get_currency("DE_LU") == "EUR"
        assert registry.get_currency("ES") == "EUR"

    def test_gbp(self, registry):
        assert registry.get_currency("GB") == "GBP"

    def test_unknown_raises(self, registry):
        with pytest.raises(KeyError):
            registry.get_currency("FAKE")


# ---- get_native_resolution ----


class TestGetNativeResolution:
    def test_aeso_price(self, registry):
        assert registry.get_native_resolution("AESO", "price") == 60

    def test_aeso_demand(self, registry):
        assert registry.get_native_resolution("AESO", "demand") == 1

    def test_de_lu_fifteen_minute(self, registry):
        assert registry.get_native_resolution("DE_LU", "price") == 15
        assert registry.get_native_resolution("DE_LU", "demand") == 15
        assert registry.get_native_resolution("DE_LU", "generation") == 15

    def test_gb_thirty_minute(self, registry):
        assert registry.get_native_resolution("GB", "price") == 30

    def test_unknown_market_raises(self, registry):
        with pytest.raises(KeyError):
            registry.get_native_resolution("FAKE", "price")

    def test_unknown_data_type_raises(self, registry):
        with pytest.raises(KeyError, match="No native resolution"):
            registry.get_native_resolution("AESO", "flows")

    def test_custom_registry(self, custom_registry):
        assert custom_registry.get_native_resolution("TEST", "price") == 60
        assert custom_registry.get_native_resolution("TEST", "demand") == 30
        assert custom_registry.get_native_resolution("TEST", "generation") == 15
