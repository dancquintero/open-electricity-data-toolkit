"""Main user-facing query interface for the electricity data toolkit."""

from __future__ import annotations

from pathlib import Path


class Toolkit:
    """Query interface for electricity market data.

    Provides methods to collect, store, and query harmonized electricity
    market data across North American and European jurisdictions.

    Parameters
    ----------
    data_dir : str or Path
        Path to the local data directory for Parquet storage.
    """

    def __init__(self, data_dir: str | Path = "./data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
