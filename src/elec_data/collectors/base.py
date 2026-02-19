"""Abstract base class for all data collectors."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseCollector(ABC):
    """Abstract base for all data collectors.

    All collectors must return DataFrames conforming to the common data
    model schemas defined in harmonize/schemas.py.
    """

    @abstractmethod
    def collect_prices(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect price data. Return DataFrame matching PriceRecord schema."""

    @abstractmethod
    def collect_demand(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect demand data. Return DataFrame matching DemandRecord schema."""

    @abstractmethod
    def collect_generation(self, market: str, start: str, end: str) -> pd.DataFrame:
        """Collect generation data. Return DataFrame matching GenerationRecord schema."""
