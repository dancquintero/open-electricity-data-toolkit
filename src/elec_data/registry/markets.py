"""Market metadata registry access.

Loads market_registry.json and provides lookup methods for market
configuration (timezone, currency, native resolution, data sources).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_REGISTRY = Path(__file__).parent / "market_registry.json"


class MarketRegistry:
    """Programmatic access to market metadata.

    Loads the JSON registry once on init and exposes typed lookup
    methods so the rest of the toolkit never needs to hardcode
    timezone, currency, or resolution values.

    Parameters
    ----------
    registry_path : Path or None
        Path to the JSON registry file. Defaults to the bundled
        ``market_registry.json`` in the same directory as this module.
    """

    def __init__(self, registry_path: Path | None = None) -> None:
        path = registry_path or _DEFAULT_REGISTRY
        with open(path) as f:
            self._data: dict[str, dict] = json.load(f)
        logger.debug("Loaded market registry with %d markets", len(self._data))

    def get(self, market: str) -> dict:
        """Return the full metadata dict for a market.

        Parameters
        ----------
        market : str
            Market identifier (e.g. ``"AESO"``).

        Returns
        -------
        dict
            All metadata fields for the market.

        Raises
        ------
        KeyError
            If the market is not in the registry.
        """
        if market not in self._data:
            raise KeyError(f"Unknown market: {market!r}")
        return dict(self._data[market])

    def list_markets(self) -> list[str]:
        """Return all registered market identifiers, sorted.

        Returns
        -------
        list[str]
            Sorted list of market keys.
        """
        return sorted(self._data.keys())

    def get_timezone(self, market: str) -> str:
        """Return the canonical timezone for a market.

        Parameters
        ----------
        market : str
            Market identifier.

        Returns
        -------
        str
            IANA timezone string (e.g. ``"America/Edmonton"``).
        """
        return self.get(market)["timezone"]

    def get_currency(self, market: str) -> str:
        """Return the currency code for a market.

        Parameters
        ----------
        market : str
            Market identifier.

        Returns
        -------
        str
            ISO 4217 currency code (e.g. ``"CAD"``).
        """
        return self.get(market)["currency"]

    def get_native_resolution(self, market: str, data_type: str) -> int:
        """Return the native resolution in minutes for a market/data_type.

        Looks up ``native_{data_type}_resolution_minutes`` from the
        registry entry.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            One of ``"price"``, ``"demand"``, ``"generation"``.

        Returns
        -------
        int
            Resolution in minutes.

        Raises
        ------
        KeyError
            If the market or resolution field is not found.
        """
        meta = self.get(market)
        key = f"native_{data_type}_resolution_minutes"
        if key not in meta:
            raise KeyError(
                f"No native resolution for {data_type!r} in market {market!r} "
                f"(expected key {key!r})"
            )
        return meta[key]
