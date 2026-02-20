"""Main user-facing query interface for the electricity data toolkit."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from elec_data.collectors.base import BaseCollector
from elec_data.collectors.gridstatus_collector import GridstatusCollector
from elec_data.registry.markets import MarketRegistry
from elec_data.storage.collection_log import CollectionLog
from elec_data.storage.parquet_store import ParquetStore

logger = logging.getLogger(__name__)

_COLLECT_METHODS: dict[str, str] = {
    "prices": "collect_prices",
    "demand": "collect_demand",
    "generation": "collect_generation",
}


class Toolkit:
    """Query interface for electricity market data.

    Provides methods to collect, store, and query harmonized electricity
    market data across North American and European jurisdictions.

    Parameters
    ----------
    data_dir : str or Path
        Path to the local data directory for Parquet storage.

    Examples
    --------
    >>> from elec_data import Toolkit
    >>> tk = Toolkit(data_dir="./data")
    >>> tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-02-01")
    >>> prices = tk.get_prices(["AESO"], "2024-01-01", "2024-02-01")
    """

    def __init__(self, data_dir: str | Path = "./data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self._store = ParquetStore(self.data_dir)
        self._log = CollectionLog(self.data_dir)
        self._registry = MarketRegistry()
        self._collectors: list[BaseCollector] = [GridstatusCollector(self._registry)]

    # ------------------------------------------------------------------ #
    # Query methods
    # ------------------------------------------------------------------ #

    def get_prices(
        self,
        markets: list[str],
        start: str,
        end: str,
        resolution: str | None = None,
        pivot: bool = False,
    ) -> pd.DataFrame:
        """Get price data for one or more markets.

        Checks local storage first; fetches missing data from APIs
        automatically.

        Parameters
        ----------
        markets : list[str]
            Market identifiers (e.g. ``["AESO", "IESO"]``).
        start : str
            Start date as ISO string (inclusive).
        end : str
            End date as ISO string (exclusive).
        resolution : str or None
            Target resolution (not implemented in MVP; returns native).
        pivot : bool
            Not implemented in MVP; returns long format.

        Returns
        -------
        pd.DataFrame
            Price data conforming to the PriceRecord schema.
        """
        if resolution is not None:
            logger.warning(
                "resolution parameter not yet implemented; returning native resolution"
            )
        if pivot:
            logger.warning("pivot parameter not yet implemented; returning long format")
        return self._get_data(markets, "prices", start, end)

    def get_demand(
        self,
        markets: list[str],
        start: str,
        end: str,
        resolution: str | None = None,
        pivot: bool = False,
    ) -> pd.DataFrame:
        """Get demand data for one or more markets.

        Parameters
        ----------
        markets : list[str]
            Market identifiers.
        start : str
            Start date (inclusive).
        end : str
            End date (exclusive).
        resolution : str or None
            Not implemented in MVP.
        pivot : bool
            Not implemented in MVP.

        Returns
        -------
        pd.DataFrame
            Demand data conforming to the DemandRecord schema.
        """
        if resolution is not None:
            logger.warning(
                "resolution parameter not yet implemented; returning native resolution"
            )
        if pivot:
            logger.warning("pivot parameter not yet implemented; returning long format")
        return self._get_data(markets, "demand", start, end)

    def get_generation(
        self,
        markets: list[str],
        start: str,
        end: str,
        fuel_types: list[str] | None = None,
        resolution: str | None = None,
    ) -> pd.DataFrame:
        """Get generation data for one or more markets.

        Parameters
        ----------
        markets : list[str]
            Market identifiers.
        start : str
            Start date (inclusive).
        end : str
            End date (exclusive).
        fuel_types : list[str] or None
            Filter to these fuel types. None returns all.
        resolution : str or None
            Not implemented in MVP.

        Returns
        -------
        pd.DataFrame
            Generation data conforming to the GenerationRecord schema.
        """
        if resolution is not None:
            logger.warning(
                "resolution parameter not yet implemented; returning native resolution"
            )
        df = self._get_data(markets, "generation", start, end)
        if fuel_types is not None and not df.empty:
            df = df[df["fuel_type"].isin(fuel_types)].reset_index(drop=True)
        return df

    # ------------------------------------------------------------------ #
    # Collection
    # ------------------------------------------------------------------ #

    def collect(
        self,
        markets: list[str],
        data_types: list[str],
        start: str,
        end: str,
    ) -> None:
        """Explicitly collect and store data from APIs.

        Fetches data for each market/data_type combination, stores it
        in Parquet, and logs the collection. Chunks large date ranges
        by month to avoid API timeouts.

        Parameters
        ----------
        markets : list[str]
            Market identifiers to collect for.
        data_types : list[str]
            Data types to collect (e.g. ``["prices", "demand"]``).
        start : str
            Start date as ISO string.
        end : str
            End date as ISO string.
        """
        chunks = self._monthly_chunks(start, end)
        tasks = [(m, dt) for m in markets for dt in data_types]
        total = len(tasks) * len(chunks)

        with tqdm(total=total, desc="Collecting data", unit="chunk") as pbar:
            for market, data_type in tasks:
                try:
                    collector = self._get_collector(market)
                    collect_fn = self._collector_method(collector, data_type)
                except ValueError:
                    logger.exception("Skipping %s/%s", market, data_type)
                    pbar.update(len(chunks))
                    continue

                for chunk_start, chunk_end in chunks:
                    pbar.set_postfix_str(f"{market}/{data_type} {chunk_start}")
                    self._collect_chunk(
                        collector_fn=collect_fn,
                        market=market,
                        data_type=data_type,
                        start=chunk_start,
                        end=chunk_end,
                    )
                    pbar.update(1)

    # ------------------------------------------------------------------ #
    # Status
    # ------------------------------------------------------------------ #

    def status(self) -> pd.DataFrame:
        """Show what data is available in the local store.

        Returns
        -------
        pd.DataFrame
            Summary with columns: market, data_type, start, end.
        """
        rows: list[dict] = []
        for market_lower in self._store.list_markets():
            market_upper = market_lower.upper()
            for data_type in self._store.list_data_types(market_upper):
                date_range = self._store.get_date_range(market_upper, data_type)
                if date_range is not None:
                    rows.append({
                        "market": market_upper,
                        "data_type": data_type,
                        "start": date_range[0],
                        "end": date_range[1],
                    })

        if not rows:
            return pd.DataFrame(columns=["market", "data_type", "start", "end"])
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    def _get_data(
        self, markets: list[str], data_type: str, start: str, end: str
    ) -> pd.DataFrame:
        """Read data from store, auto-fetching from API if missing."""
        frames: list[pd.DataFrame] = []

        for market in markets:
            stored = self._store.read(market, data_type, start, end)
            if stored.empty:
                stored = self._auto_fetch(market, data_type, start, end)
            if not stored.empty:
                frames.append(stored)

        if not frames:
            return self._empty_df(data_type)

        return pd.concat(frames, ignore_index=True).sort_values(
            "timestamp_utc"
        ).reset_index(drop=True)

    def _auto_fetch(
        self, market: str, data_type: str, start: str, end: str
    ) -> pd.DataFrame:
        """Attempt to fetch, store, and return data for a missing range."""
        try:
            collector = self._get_collector(market)
            collect_fn = self._collector_method(collector, data_type)
            fetched = collect_fn(market, start, end)

            if not fetched.empty:
                self._store_dataframe(fetched, market, data_type)
                source = fetched["source"].iloc[0] if "source" in fetched.columns else "unknown"
                self._log.log(
                    market=market,
                    data_type=data_type,
                    start=pd.Timestamp(start, tz="UTC").to_pydatetime(),
                    end=pd.Timestamp(end, tz="UTC").to_pydatetime(),
                    rows=len(fetched),
                    source=source,
                )
                return self._store.read(market, data_type, start, end)

        except Exception:
            logger.exception("Failed to auto-fetch %s/%s for %s–%s", market, data_type, start, end)

        return self._empty_df(data_type)

    def _collect_chunk(
        self,
        collector_fn,
        market: str,
        data_type: str,
        start: str,
        end: str,
    ) -> None:
        """Fetch a single chunk, store it, and log the result."""
        try:
            df = collector_fn(market, start, end)
            if not df.empty:
                self._store_dataframe(df, market, data_type)
                source = df["source"].iloc[0] if "source" in df.columns else "unknown"
                self._log.log(
                    market=market,
                    data_type=data_type,
                    start=pd.Timestamp(start, tz="UTC").to_pydatetime(),
                    end=pd.Timestamp(end, tz="UTC").to_pydatetime(),
                    rows=len(df),
                    source=source,
                )
            else:
                logger.info(
                    "No data returned for %s/%s %s–%s", market, data_type, start, end
                )
        except Exception:
            logger.exception(
                "Failed to collect %s/%s for %s–%s", market, data_type, start, end
            )
            self._log.log(
                market=market,
                data_type=data_type,
                start=pd.Timestamp(start, tz="UTC").to_pydatetime(),
                end=pd.Timestamp(end, tz="UTC").to_pydatetime(),
                rows=0,
                source="unknown",
                status="error",
            )

    def _get_collector(self, market: str) -> BaseCollector:
        """Find a collector that supports the given market."""
        for collector in self._collectors:
            if market in collector.supported_markets:
                return collector

        all_supported: list[str] = []
        for c in self._collectors:
            all_supported.extend(c.supported_markets)
        raise ValueError(
            f"Market {market!r} is not supported. "
            f"Supported markets: {sorted(set(all_supported))}"
        )

    @staticmethod
    def _collector_method(collector: BaseCollector, data_type: str):
        """Return the bound collector method for a given data type."""
        method_name = _COLLECT_METHODS.get(data_type)
        if method_name is None:
            raise ValueError(
                f"Data type {data_type!r} not supported. "
                f"Supported: {sorted(_COLLECT_METHODS)}"
            )
        return getattr(collector, method_name)

    def _store_dataframe(self, df: pd.DataFrame, market: str, data_type: str) -> None:
        """Split a DataFrame by year and write each chunk to the store."""
        if df.empty:
            return
        for year, group in df.groupby(df["timestamp_utc"].dt.year):
            self._store.write(group, market, data_type, int(year))

    @staticmethod
    def _monthly_chunks(start: str, end: str) -> list[tuple[str, str]]:
        """Split a date range into month-sized chunks."""
        chunks: list[tuple[str, str]] = []
        current = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)

        while current < end_ts:
            next_month = current + pd.offsets.MonthBegin(1)
            chunk_end = min(next_month, end_ts)
            chunks.append((
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            ))
            current = chunk_end

        return chunks

    @staticmethod
    def _empty_df(data_type: str) -> pd.DataFrame:
        """Return an empty DataFrame with the correct columns for a data type."""
        from elec_data.harmonize.schemas import SCHEMA_MAP

        schema_class = SCHEMA_MAP.get(data_type)
        if schema_class is None:
            return pd.DataFrame()
        return pd.DataFrame(columns=list(schema_class.model_fields.keys()))
