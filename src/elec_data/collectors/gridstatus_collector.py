"""Gridstatus-based collector for North American ISOs (AESO, IESO).

Wraps the gridstatus library to fetch electricity market data and
transform it into the common data model schemas.
"""

from __future__ import annotations

import logging
import os

import pandas as pd

from elec_data.collectors.base import BaseCollector
from elec_data.registry.markets import MarketRegistry

logger = logging.getLogger(__name__)

# Mapping from gridstatus fuel type names to our FuelType enum values.
# AESO fuel types (from get_fuel_mix, wide-format columns):
#   Cogeneration, Combined Cycle, Gas Fired Steam, Simple Cycle → gas
#   Hydro → hydro, Wind → wind, Solar → solar
#   Energy Storage → storage, Other → other
# IESO fuel types (from get_fuel_mix, wide-format columns):
#   Gas → gas, Hydro → hydro, Nuclear → nuclear
#   Wind → wind, Solar → solar, Biofuel → biomass, Other → other

AESO_FUEL_MAP: dict[str, str] = {
    "Cogeneration": "gas",
    "Combined Cycle": "gas",
    "Gas Fired Steam": "gas",
    "Simple Cycle": "gas",
    "Coal": "coal",
    "Hydro": "hydro",
    "Wind": "wind",
    "Solar": "solar",
    "Energy Storage": "storage",
    "Other": "other",
}

IESO_FUEL_MAP: dict[str, str] = {
    "Gas": "gas",
    "Hydro": "hydro",
    "Nuclear": "nuclear",
    "Wind": "wind",
    "Solar": "solar",
    "Biofuel": "biomass",
    "Other": "other",
}


class GridstatusCollector(BaseCollector):
    """Collects electricity data from North American ISOs via gridstatus.

    Currently supports AESO (Alberta) and IESO (Ontario). ISO client
    objects are lazily initialized so you don't need every API key to
    use markets that don't require one.

    Parameters
    ----------
    registry : MarketRegistry or None
        Market metadata registry. If None, uses the default bundled
        registry. Provides timezone, currency, and resolution lookups.
    """

    def __init__(self, registry: MarketRegistry | None = None) -> None:
        self._registry = registry or MarketRegistry()
        self._iso_cache: dict[str, object] = {}

    @property
    def supported_markets(self) -> list[str]:
        return ["AESO", "IESO"]

    # ---- Public API ----

    def collect_prices(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect price data for a market over a date range.

        Parameters
        ----------
        market : str
            Market identifier (``"AESO"`` or ``"IESO"``).
        start, end : str
            ISO date strings (e.g. ``"2024-01-01"``).

        Returns
        -------
        pd.DataFrame
            DataFrame conforming to ``PriceRecord`` schema.
        """
        self._validate_market(market)
        iso = self._get_iso(market)
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)

        if market == "AESO":
            raw = iso.get_pool_price(start_ts, end_ts)
            return self._transform_aeso_prices(raw)
        else:  # IESO
            raw = iso.get_hoep_historical_hourly(start_ts, end_ts)
            return self._transform_ieso_prices(raw)

    def collect_demand(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect demand data for a market over a date range.

        Parameters
        ----------
        market : str
            Market identifier.
        start, end : str
            ISO date strings.

        Returns
        -------
        pd.DataFrame
            DataFrame conforming to ``DemandRecord`` schema.
        """
        self._validate_market(market)
        iso = self._get_iso(market)
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)

        if market == "AESO":
            raw = iso.get_load(start_ts, end_ts)
            return self._transform_aeso_demand(raw)
        else:  # IESO
            raw = iso.get_load_zonal_hourly(start_ts, end_ts)
            return self._transform_ieso_demand(raw)

    def collect_generation(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect generation data for a market over a date range.

        Parameters
        ----------
        market : str
            Market identifier.
        start, end : str
            ISO date strings.

        Returns
        -------
        pd.DataFrame
            DataFrame conforming to ``GenerationRecord`` schema.

        Notes
        -----
        AESO ``get_fuel_mix()`` only returns current/real-time data.
        For historical AESO generation, wind and solar hourly data is
        available via separate endpoints. This method uses
        ``get_fuel_mix()`` for current snapshots.
        """
        self._validate_market(market)
        iso = self._get_iso(market)
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)

        if market == "AESO":
            # AESO fuel mix is real-time only. For historical data,
            # we can get wind and solar hourly. Attempt fuel_mix first;
            # if it fails or returns empty, fall back.
            raw = iso.get_fuel_mix()
            return self._transform_aeso_generation(raw)
        else:  # IESO
            raw = iso.get_fuel_mix(start_ts, end_ts)
            return self._transform_ieso_generation(raw)

    # ---- ISO client management ----

    def _get_iso(self, market: str) -> object:
        """Lazily create and cache the gridstatus ISO client."""
        if market not in self._iso_cache:
            if market == "AESO":
                from gridstatus import AESO

                api_key = os.getenv("AESO_API_KEY")
                if not api_key:
                    raise ValueError(
                        "AESO_API_KEY environment variable is required. "
                        "Set it in your .env file or environment."
                    )
                self._iso_cache[market] = AESO(api_key=api_key)
                logger.info("Initialized AESO client")
            elif market == "IESO":
                from gridstatus import IESO

                self._iso_cache[market] = IESO()
                logger.info("Initialized IESO client")
        return self._iso_cache[market]

    # ---- AESO transformations ----

    def _transform_aeso_prices(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus AESO pool price data to PriceRecord schema.

        gridstatus returns: Interval Start, Interval End, Pool Price,
        Rolling 30 Day Average Pool Price
        """
        if raw.empty:
            return self._empty_price_df()

        df = pd.DataFrame(
            {
                "timestamp_utc": raw["Interval Start"].dt.tz_convert("UTC"),
                "market": "AESO",
                "price": raw["Pool Price"],
                "currency": self._registry.get_currency("AESO"),
                "price_type": "pool",
                "resolution_minutes": self._registry.get_native_resolution(
                    "AESO", "price"
                ),
                "source": "gridstatus_aeso",
            }
        )
        return df.sort_values("timestamp_utc").reset_index(drop=True)

    def _transform_aeso_demand(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus AESO load data to DemandRecord schema.

        gridstatus returns: Interval Start, Interval End, Load
        """
        if raw.empty:
            return self._empty_demand_df()

        df = pd.DataFrame(
            {
                "timestamp_utc": raw["Interval Start"].dt.tz_convert("UTC"),
                "market": "AESO",
                "demand_mw": raw["Load"],
                "demand_type": "actual",
                "resolution_minutes": self._registry.get_native_resolution(
                    "AESO", "demand"
                ),
                "source": "gridstatus_aeso",
            }
        )
        return df.sort_values("timestamp_utc").reset_index(drop=True)

    def _transform_aeso_generation(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus AESO fuel mix data to GenerationRecord schema.

        gridstatus returns a wide table with columns: Time, Cogeneration,
        Combined Cycle, Energy Storage, Gas Fired Steam, Hydro, Other,
        Simple Cycle, Solar, Wind
        """
        if raw.empty:
            return self._empty_generation_df()

        # Identify the time column — fuel_mix uses "Time"
        time_col = "Time" if "Time" in raw.columns else "Interval Start"

        # Melt the wide fuel columns into long format
        fuel_cols = [c for c in raw.columns if c in AESO_FUEL_MAP]
        if not fuel_cols:
            return self._empty_generation_df()

        melted = raw.melt(
            id_vars=[time_col],
            value_vars=fuel_cols,
            var_name="raw_fuel",
            value_name="generation_mw",
        )
        melted["fuel_type"] = melted["raw_fuel"].map(AESO_FUEL_MAP)

        # Aggregate fuel types that map to the same standard type
        # (e.g. Cogeneration + Combined Cycle + Gas Fired Steam + Simple Cycle → gas)
        ts = melted[time_col].dt.tz_convert("UTC")
        grouped = (
            melted.assign(timestamp_utc=ts)
            .groupby(["timestamp_utc", "fuel_type"], as_index=False)["generation_mw"]
            .sum()
        )

        grouped["market"] = "AESO"
        grouped["resolution_minutes"] = self._registry.get_native_resolution(
            "AESO", "generation"
        )
        grouped["source"] = "gridstatus_aeso"

        return (
            grouped[
                [
                    "timestamp_utc",
                    "market",
                    "fuel_type",
                    "generation_mw",
                    "resolution_minutes",
                    "source",
                ]
            ]
            .sort_values(["timestamp_utc", "fuel_type"])
            .reset_index(drop=True)
        )

    # ---- IESO transformations ----

    def _transform_ieso_prices(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus IESO HOEP data to PriceRecord schema.

        gridstatus returns: Interval Start, Interval End, HOEP, plus
        predispatch and operating reserve columns.
        """
        if raw.empty:
            return self._empty_price_df()

        df = pd.DataFrame(
            {
                "timestamp_utc": raw["Interval Start"].dt.tz_convert("UTC"),
                "market": "IESO",
                "price": raw["HOEP"],
                "currency": self._registry.get_currency("IESO"),
                "price_type": "pool",
                "resolution_minutes": self._registry.get_native_resolution(
                    "IESO", "price"
                ),
                "source": "gridstatus_ieso",
            }
        )
        return df.sort_values("timestamp_utc").reset_index(drop=True)

    def _transform_ieso_demand(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus IESO zonal load data to DemandRecord schema.

        gridstatus returns: Interval Start, Interval End, Ontario Demand,
        plus zonal columns (Northwest, Northeast, etc.).
        """
        if raw.empty:
            return self._empty_demand_df()

        df = pd.DataFrame(
            {
                "timestamp_utc": raw["Interval Start"].dt.tz_convert("UTC"),
                "market": "IESO",
                "demand_mw": raw["Ontario Demand"],
                "demand_type": "actual",
                "resolution_minutes": self._registry.get_native_resolution(
                    "IESO", "demand"
                ),
                "source": "gridstatus_ieso",
            }
        )
        return df.sort_values("timestamp_utc").reset_index(drop=True)

    def _transform_ieso_generation(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Transform gridstatus IESO fuel mix data to GenerationRecord schema.

        gridstatus returns a wide table: Interval Start, Interval End,
        Biofuel, Gas, Hydro, Nuclear, Solar, Wind, Other
        """
        if raw.empty:
            return self._empty_generation_df()

        fuel_cols = [c for c in raw.columns if c in IESO_FUEL_MAP]
        if not fuel_cols:
            return self._empty_generation_df()

        melted = raw.melt(
            id_vars=["Interval Start"],
            value_vars=fuel_cols,
            var_name="raw_fuel",
            value_name="generation_mw",
        )
        melted["fuel_type"] = melted["raw_fuel"].map(IESO_FUEL_MAP)

        ts = melted["Interval Start"].dt.tz_convert("UTC")
        df = pd.DataFrame(
            {
                "timestamp_utc": ts,
                "market": "IESO",
                "fuel_type": melted["fuel_type"],
                "generation_mw": melted["generation_mw"],
                "resolution_minutes": self._registry.get_native_resolution(
                    "IESO", "generation"
                ),
                "source": "gridstatus_ieso",
            }
        )

        # Drop rows where generation_mw is NaN (e.g. "Other" in old data)
        df = df.dropna(subset=["generation_mw"])

        return df.sort_values(["timestamp_utc", "fuel_type"]).reset_index(drop=True)

    # ---- Empty DataFrame factories ----

    @staticmethod
    def _empty_price_df() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "timestamp_utc",
                "market",
                "price",
                "currency",
                "price_type",
                "resolution_minutes",
                "source",
            ]
        )

    @staticmethod
    def _empty_demand_df() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "timestamp_utc",
                "market",
                "demand_mw",
                "demand_type",
                "resolution_minutes",
                "source",
            ]
        )

    @staticmethod
    def _empty_generation_df() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "timestamp_utc",
                "market",
                "fuel_type",
                "generation_mw",
                "resolution_minutes",
                "source",
            ]
        )
