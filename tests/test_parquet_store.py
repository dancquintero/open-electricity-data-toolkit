"""Tests for elec_data.storage.parquet_store.ParquetStore."""

from __future__ import annotations

import pandas as pd

from elec_data.storage.parquet_store import ParquetStore


class TestInit:
    def test_sets_data_dir(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        assert store.data_dir == tmp_path
        assert store.raw_dir == tmp_path / "raw"


class TestWrite:
    def test_creates_parquet_file(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        path = store.write(sample_price_df, "AESO", "prices", 2024)
        assert path.exists()
        assert path == tmp_path / "raw" / "aeso" / "prices" / "2024.parquet"

    def test_creates_directories(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        assert (tmp_path / "raw" / "aeso" / "prices").is_dir()

    def test_market_lowercase_in_path(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        path = store.write(sample_price_df, "AESO", "prices", 2024)
        assert "aeso" in str(path)

    def test_preserves_data(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "prices" / "2024.parquet")
        assert len(result) == 3

    def test_append_deduplicates(self, tmp_path, sample_price_df):
        """Writing the same data twice should not double the rows."""
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "prices" / "2024.parquet")
        assert len(result) == 3

    def test_append_keeps_new_timestamps(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)

        new_df = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2024-01-01 03:00"], utc=True),
            "market": ["AESO"],
            "price": [55.0],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["gridstatus_aeso"],
        })
        store.write(new_df, "AESO", "prices", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "prices" / "2024.parquet")
        assert len(result) == 4

    def test_append_overwrites_duplicate_timestamp(self, tmp_path, sample_price_df):
        """Duplicate timestamp keeps the last (newer) value."""
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)

        updated = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2024-01-01 00:00"], utc=True),
            "market": ["AESO"],
            "price": [999.99],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["gridstatus_aeso"],
        })
        store.write(updated, "AESO", "prices", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "prices" / "2024.parquet")
        row = result[result["timestamp_utc"] == pd.Timestamp("2024-01-01", tz="UTC")]
        assert row.iloc[0]["price"] == 999.99

    def test_result_sorted(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        df = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(
                ["2024-01-01 02:00", "2024-01-01 00:00", "2024-01-01 01:00"],
                utc=True,
            ),
            "market": ["AESO"] * 3,
            "price": [1.0, 2.0, 3.0],
            "currency": ["CAD"] * 3,
            "price_type": ["pool"] * 3,
            "resolution_minutes": [60] * 3,
            "source": ["test"] * 3,
        })
        store.write(df, "AESO", "prices", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "prices" / "2024.parquet")
        assert result["timestamp_utc"].is_monotonic_increasing

    def test_generation_multiple_fuels_per_timestamp(self, tmp_path, sample_generation_df):
        """Generation data has multiple fuel types per timestamp — all should be kept."""
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_generation_df, "AESO", "generation", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "generation" / "2024.parquet")
        # 2 timestamps x 2 fuel types = 4 rows
        assert len(result) == 4

    def test_generation_dedup_preserves_fuel_types(self, tmp_path, sample_generation_df):
        """Re-writing generation data should still keep all fuel type rows."""
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_generation_df, "AESO", "generation", 2024)
        store.write(sample_generation_df, "AESO", "generation", 2024)
        result = pd.read_parquet(tmp_path / "raw" / "aeso" / "generation" / "2024.parquet")
        assert len(result) == 4


class TestRead:
    def test_returns_data(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = store.read("AESO", "prices", "2024-01-01", "2024-02-01")
        assert len(result) == 3

    def test_filters_by_date_range(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = store.read("AESO", "prices", "2024-01-01 00:00", "2024-01-01 02:00")
        assert len(result) == 2

    def test_end_is_exclusive(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = store.read("AESO", "prices", "2024-01-01 00:00", "2024-01-01 01:00")
        assert len(result) == 1

    def test_no_data_returns_empty_df(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        result = store.read("AESO", "prices", "2024-01-01", "2024-02-01")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        # Should have correct columns
        assert "timestamp_utc" in result.columns
        assert "price" in result.columns

    def test_across_years(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)

        df_2023 = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2023-12-31 23:00"], utc=True),
            "market": ["AESO"],
            "price": [40.0],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["test"],
        })
        df_2024 = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2024-01-01 00:00"], utc=True),
            "market": ["AESO"],
            "price": [50.0],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["test"],
        })
        store.write(df_2023, "AESO", "prices", 2023)
        store.write(df_2024, "AESO", "prices", 2024)

        result = store.read("AESO", "prices", "2023-12-31", "2024-01-02")
        assert len(result) == 2

    def test_result_sorted(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = store.read("AESO", "prices", "2024-01-01", "2024-02-01")
        assert result["timestamp_utc"].is_monotonic_increasing


class TestGetDateRange:
    def test_returns_min_max(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        result = store.get_date_range("AESO", "prices")
        assert result is not None
        min_ts, max_ts = result
        assert min_ts == pd.Timestamp("2024-01-01 00:00", tz="UTC")
        assert max_ts == pd.Timestamp("2024-01-01 02:00", tz="UTC")

    def test_returns_none_no_data(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        assert store.get_date_range("AESO", "prices") is None

    def test_spans_multiple_years(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        df_2023 = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2023-06-15 12:00"], utc=True),
            "market": ["AESO"],
            "price": [40.0],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["test"],
        })
        df_2024 = pd.DataFrame({
            "timestamp_utc": pd.to_datetime(["2024-03-20 08:00"], utc=True),
            "market": ["AESO"],
            "price": [50.0],
            "currency": ["CAD"],
            "price_type": ["pool"],
            "resolution_minutes": [60],
            "source": ["test"],
        })
        store.write(df_2023, "AESO", "prices", 2023)
        store.write(df_2024, "AESO", "prices", 2024)
        result = store.get_date_range("AESO", "prices")
        assert result is not None
        assert result[0] == pd.Timestamp("2023-06-15 12:00", tz="UTC")
        assert result[1] == pd.Timestamp("2024-03-20 08:00", tz="UTC")


class TestListMethods:
    def test_list_markets_empty(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        assert store.list_markets() == []

    def test_list_markets_returns_stored(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        assert "aeso" in store.list_markets()

    def test_list_data_types_empty(self, tmp_path):
        store = ParquetStore(data_dir=tmp_path)
        assert store.list_data_types("AESO") == []

    def test_list_data_types_returns_stored(self, tmp_path, sample_price_df):
        store = ParquetStore(data_dir=tmp_path)
        store.write(sample_price_df, "AESO", "prices", 2024)
        assert "prices" in store.list_data_types("AESO")
