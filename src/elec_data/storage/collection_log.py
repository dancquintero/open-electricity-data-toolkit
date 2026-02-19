"""Collection log tracking what data has been collected and when.

Stores collection events in data/metadata/collection_log.parquet to
enable resumable backfills and gap detection.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_LOG_COLUMNS = [
    "market",
    "data_type",
    "start_date",
    "end_date",
    "rows_collected",
    "collected_at",
    "source",
    "status",
]


class CollectionLog:
    """Tracks collection events for resumable backfills and gap detection.

    Stores log entries in ``data/metadata/collection_log.parquet``.
    Each call to :meth:`log` appends a row recording what was collected,
    when, and whether it succeeded.

    Parameters
    ----------
    data_dir : Path
        Root data directory (the one containing ``metadata/``).
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.log_path = self.data_dir / "metadata" / "collection_log.parquet"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        market: str,
        data_type: str,
        start: datetime,
        end: datetime,
        rows: int,
        source: str,
        status: str = "success",
    ) -> None:
        """Record a collection event.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            Data type collected.
        start : datetime
            Start of the collected date range (UTC-aware).
        end : datetime
            End of the collected date range (UTC-aware).
        rows : int
            Number of rows fetched.
        source : str
            Data source identifier.
        status : str
            "success" or "error".
        """
        existing = self._read_log()
        new_row = pd.DataFrame([{
            "market": market,
            "data_type": data_type,
            "start_date": start,
            "end_date": end,
            "rows_collected": rows,
            "collected_at": datetime.now(timezone.utc),
            "source": source,
            "status": status,
        }])
        if existing.empty:
            combined = new_row
        else:
            combined = pd.concat([existing, new_row], ignore_index=True)
        self._write_log(combined)
        logger.info(
            "Logged %s collection for %s/%s: %s to %s (%d rows)",
            status, market, data_type, start, end, rows,
        )

    def get_latest(self, market: str, data_type: str) -> datetime | None:
        """Return the latest end_date for successful collections.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            Data type.

        Returns
        -------
        datetime or None
            Latest end_date, or None if no successful collections exist.
        """
        log = self._read_log()
        if log.empty:
            return None

        mask = (
            (log["market"] == market)
            & (log["data_type"] == data_type)
            & (log["status"] == "success")
        )
        filtered = log.loc[mask]
        if filtered.empty:
            return None

        return filtered["end_date"].max().to_pydatetime()

    def get_gaps(
        self,
        market: str,
        data_type: str,
        expected_start: datetime,
        expected_end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        """Find date ranges with no successful collection.

        Compares the expected full range against successfully collected
        ranges to identify gaps.

        Parameters
        ----------
        market : str
            Market identifier.
        data_type : str
            Data type.
        expected_start : datetime
            Expected start of full coverage (UTC-aware).
        expected_end : datetime
            Expected end of full coverage (UTC-aware).

        Returns
        -------
        list[tuple[datetime, datetime]]
            List of ``(gap_start, gap_end)`` tuples. Empty if fully covered.
        """
        log = self._read_log()

        mask = (
            (log["market"] == market)
            & (log["data_type"] == data_type)
            & (log["status"] == "success")
        )
        filtered = log.loc[mask]

        if filtered.empty:
            return [(expected_start, expected_end)]

        # Sort and merge overlapping/adjacent ranges
        ranges = sorted(zip(filtered["start_date"], filtered["end_date"]))
        merged = [ranges[0]]
        for start, end in ranges[1:]:
            prev_start, prev_end = merged[-1]
            if start <= prev_end:
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))

        # Find gaps between merged ranges and the expected boundaries
        gaps = []
        cursor = expected_start
        for covered_start, covered_end in merged:
            # Convert to comparable types
            covered_start = pd.Timestamp(covered_start).to_pydatetime()
            covered_end = pd.Timestamp(covered_end).to_pydatetime()

            if cursor < covered_start:
                gaps.append((cursor, covered_start))
            cursor = max(cursor, covered_end)

        if cursor < expected_end:
            gaps.append((cursor, expected_end))

        return gaps

    def status(self) -> pd.DataFrame:
        """Summary of all markets, types, date ranges, and freshness.

        Returns
        -------
        pd.DataFrame
            One row per ``(market, data_type)`` with columns: market,
            data_type, earliest, latest, total_rows, last_collected.
        """
        log = self._read_log()
        status_cols = ["market", "data_type", "earliest", "latest", "total_rows", "last_collected"]

        if log.empty:
            return pd.DataFrame(columns=status_cols)

        success = log.loc[log["status"] == "success"]
        if success.empty:
            return pd.DataFrame(columns=status_cols)

        summary = (
            success.groupby(["market", "data_type"])
            .agg(
                earliest=("start_date", "min"),
                latest=("end_date", "max"),
                total_rows=("rows_collected", "sum"),
                last_collected=("collected_at", "max"),
            )
            .reset_index()
        )
        return summary

    # -- Private helpers --

    def _read_log(self) -> pd.DataFrame:
        """Read the log file, returning empty DataFrame if it doesn't exist."""
        if not self.log_path.exists():
            return pd.DataFrame(columns=_LOG_COLUMNS)
        return pq.read_table(self.log_path).to_pandas()

    def _write_log(self, df: pd.DataFrame) -> None:
        """Write the log DataFrame to Parquet."""
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, self.log_path)
