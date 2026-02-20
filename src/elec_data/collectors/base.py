"""Abstract base class for all data collectors."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseCollector(ABC):
    """Abstract base for all data collectors.

    All collectors must return DataFrames conforming to the common data
    model schemas defined in harmonize/schemas.py.
    """

    @property
    @abstractmethod
    def supported_markets(self) -> list[str]:
        """Return the list of market identifiers this collector handles."""

    @abstractmethod
    def collect_prices(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect price data. Return DataFrame matching PriceRecord schema."""

    @abstractmethod
    def collect_demand(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect demand data. Return DataFrame matching DemandRecord schema."""

    @abstractmethod
    def collect_generation(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect generation data. Return DataFrame matching GenerationRecord schema."""

    def _validate_market(self, market: str) -> None:
        """Raise ValueError if the market is not supported by this collector."""
        if market not in self.supported_markets:
            raise ValueError(
                f"Market {market!r} not supported by {type(self).__name__}. "
                f"Supported: {self.supported_markets}"
            )
