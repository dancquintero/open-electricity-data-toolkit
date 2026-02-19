"""Parquet file storage for electricity market data.

Manages the data/raw/ directory structure, handling read/write/append
operations on year-partitioned Parquet files.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Value columns that should not be part of the deduplication key.
# Everything else (timestamps, identifiers, metadata) forms the composite key.
_VALUE_COLUMNS = {"price", "demand_mw", "generation_mw", "flow_mw"}


class ParquetStore:
    """Year-partitioned Parquet storage for electricity market data.

    Stores data at ``data/raw/{market}/{data_type}/{year}.parquet`` where
    market names are lowercased in the file path. Handles append-with-
    deduplication so that re-collecting the same time range is safe.

    Parameters
    ----------
    data_dir : Path
        Root data directory (the one containing ``raw/``).
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"

    def write(self, df: pd.DataFrame, market: str, data_type: str, year: int) -> Path:
        """Write or append a DataFrame to a year-partitioned Parquet file.

        If the target file already exists, reads it, concatenates with the
        new data, deduplicates on the composite key columns (everything
        except the value column), keeps the last occurrence, sorts by
        ``timestamp_utc``, and rewrites the file.

        Parameters
        ----------
        df : pd.DataFrame
            Data to write. Must contain a ``timestamp_utc`` column.
        market : str
            Market identifier (e.g. "AESO"). Lowercased in file path.
        data_type : str
            Data type (e.g. "prices", "demand").
        year : int
            Year for the partition file name.

        Returns
        -------
        Path
            Path to the written Parquet file.
        """
        path = self._parquet_path(market, data_type, year)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            existing = pq.read_table(path).to_pandas()
            combined = pd.concat([existing, df], ignore_index=True)
            key_cols = [c for c in combined.columns if c not in _VALUE_COLUMNS]
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
        else:
            combined = df.copy()

        combined = combined.sort_values("timestamp_utc").reset_index(drop=True)
        table = pa.Table.from_pandas(combined, preserve_index=False)
        pq.write_table(table, path)

        logger.info("Wrote %d rows to %s", len(combined), path)
        return path

    def read(self, market: str, data_type: str, start: str, end: str) -> pd.DataFrame:
        """Read data for a market/type within a date range.

        Determines which year files overlap with ``[start, end)``, reads
        them, and filters to rows within the range. End is exclusive.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            Data type.
        start : str
            Start date as ISO string (inclusive).
        end : str
            End date as ISO string (exclusive).

        Returns
        -------
        pd.DataFrame
            Filtered data sorted by ``timestamp_utc``. Empty DataFrame with
            correct columns if no data exists.
        """
        start_ts = pd.Timestamp(start, tz="UTC")
        end_ts = pd.Timestamp(end, tz="UTC")

        frames = []
        for year in self._year_range(start, end):
            path = self._parquet_path(market, data_type, year)
            if path.exists():
                frames.append(pq.read_table(path).to_pandas())

        if not frames:
            return self._empty_dataframe(data_type)

        combined = pd.concat(frames, ignore_index=True)
        mask = (combined["timestamp_utc"] >= start_ts) & (combined["timestamp_utc"] < end_ts)
        result = combined.loc[mask].sort_values("timestamp_utc").reset_index(drop=True)
        return result

    def get_date_range(
        self, market: str, data_type: str
    ) -> tuple[datetime, datetime] | None:
        """Return the earliest and latest timestamps stored.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            Data type.

        Returns
        -------
        tuple[datetime, datetime] or None
            ``(min_timestamp, max_timestamp)``, or ``None`` if no data.
        """
        market_dir = self._market_dir(market, data_type)
        if not market_dir.exists():
            return None

        parquet_files = sorted(market_dir.glob("*.parquet"))
        if not parquet_files:
            return None

        frames = [pq.read_table(f).to_pandas() for f in parquet_files]
        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return None

        return (
            combined["timestamp_utc"].min().to_pydatetime(),
            combined["timestamp_utc"].max().to_pydatetime(),
        )

    def list_markets(self) -> list[str]:
        """List all markets that have stored data.

        Returns
        -------
        list[str]
            Market directory names (lowercase, as stored on disk).
        """
        if not self.raw_dir.exists():
            return []
        return sorted(d.name for d in self.raw_dir.iterdir() if d.is_dir())

    def list_data_types(self, market: str) -> list[str]:
        """List data types available for a given market.

        Parameters
        ----------
        market : str
            Market identifier.

        Returns
        -------
        list[str]
            Data type directory names.
        """
        market_dir = self.raw_dir / market.lower()
        if not market_dir.exists():
            return []
        return sorted(d.name for d in market_dir.iterdir() if d.is_dir())

    # -- Private helpers --

    def _market_dir(self, market: str, data_type: str) -> Path:
        """Build path: raw/{market_lower}/{data_type}/"""
        return self.raw_dir / market.lower() / data_type

    def _parquet_path(self, market: str, data_type: str, year: int) -> Path:
        """Build path: raw/{market_lower}/{data_type}/{year}.parquet"""
        return self._market_dir(market, data_type) / f"{year}.parquet"

    def _year_range(self, start: str, end: str) -> list[int]:
        """Determine which years a date range spans."""
        start_year = pd.Timestamp(start, tz="UTC").year
        end_year = pd.Timestamp(end, tz="UTC").year
        return list(range(start_year, end_year + 1))

    def _empty_dataframe(self, data_type: str) -> pd.DataFrame:
        """Return an empty DataFrame with the correct columns for a data type."""
        from elec_data.harmonize.schemas import SCHEMA_MAP

        schema_class = SCHEMA_MAP.get(data_type)
        if schema_class is None:
            return pd.DataFrame()
        return pd.DataFrame(columns=list(schema_class.model_fields.keys()))
