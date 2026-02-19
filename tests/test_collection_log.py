"""Tests for elec_data.storage.collection_log.CollectionLog."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from elec_data.storage.collection_log import CollectionLog


def _utc(year, month, day):
    """Shorthand for UTC-aware datetime."""
    return datetime(year, month, day, tzinfo=timezone.utc)


class TestInit:
    def test_creates_metadata_directory(self, tmp_path):
        CollectionLog(data_dir=tmp_path)
        assert (tmp_path / "metadata").is_dir()

    def test_log_path_set(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        assert log.log_path == tmp_path / "metadata" / "collection_log.parquet"


class TestLog:
    def test_creates_file(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        assert log.log_path.exists()

    def test_appends_entries(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        log.log("AESO", "prices", _utc(2024, 2, 1), _utc(2024, 2, 29), 696, "gridstatus_aeso")
        df = pd.read_parquet(log.log_path)
        assert len(df) == 2

    def test_default_status_success(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        df = pd.read_parquet(log.log_path)
        assert df.iloc[0]["status"] == "success"

    def test_error_status(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log(
            "AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31),
            0, "gridstatus_aeso", status="error",
        )
        df = pd.read_parquet(log.log_path)
        assert df.iloc[0]["status"] == "error"

    def test_records_collected_at(self, tmp_path):
        before = datetime.now(timezone.utc)
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        df = pd.read_parquet(log.log_path)
        collected = pd.Timestamp(df.iloc[0]["collected_at"])
        assert collected >= pd.Timestamp(before)


class TestGetLatest:
    def test_returns_none_when_empty(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        assert log.get_latest("AESO", "prices") is None

    def test_returns_latest_end_date(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        log.log("AESO", "prices", _utc(2024, 2, 1), _utc(2024, 2, 29), 696, "gridstatus_aeso")
        latest = log.get_latest("AESO", "prices")
        assert latest == _utc(2024, 2, 29)

    def test_ignores_error_entries(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        log.log(
            "AESO", "prices", _utc(2024, 2, 1), _utc(2024, 3, 31),
            0, "gridstatus_aeso", status="error",
        )
        latest = log.get_latest("AESO", "prices")
        assert latest == _utc(2024, 1, 31)

    def test_filters_by_market_and_type(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 6, 30), 4380, "gridstatus_aeso")
        log.log("GB", "demand", _utc(2024, 1, 1), _utc(2024, 12, 31), 17520, "bmrs")
        assert log.get_latest("GB", "prices") is None


class TestGetGaps:
    def test_full_gap_when_no_data(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31))
        assert len(gaps) == 1
        assert gaps[0] == (_utc(2024, 1, 1), _utc(2024, 12, 31))

    def test_no_gaps_when_fully_covered(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31), 8760, "gridstatus_aeso")
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31))
        assert gaps == []

    def test_gap_in_middle(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 3, 31), 2160, "gridstatus_aeso")
        log.log("AESO", "prices", _utc(2024, 7, 1), _utc(2024, 12, 31), 4416, "gridstatus_aeso")
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31))
        assert len(gaps) == 1
        assert gaps[0] == (_utc(2024, 3, 31), _utc(2024, 7, 1))

    def test_gap_at_start(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 6, 1), _utc(2024, 12, 31), 5136, "gridstatus_aeso")
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31))
        assert len(gaps) == 1
        assert gaps[0][0] == _utc(2024, 1, 1)
        assert gaps[0][1] == _utc(2024, 6, 1)

    def test_gap_at_end(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 6, 1), 3624, "gridstatus_aeso")
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 12, 31))
        assert len(gaps) == 1
        assert gaps[0] == (_utc(2024, 6, 1), _utc(2024, 12, 31))

    def test_overlapping_collections_merged(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 4, 1), 2184, "gridstatus_aeso")
        log.log("AESO", "prices", _utc(2024, 3, 1), _utc(2024, 6, 1), 2208, "gridstatus_aeso")
        gaps = log.get_gaps("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 6, 1))
        assert gaps == []


class TestStatus:
    def test_empty_returns_empty_df(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        result = log.status()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_summarizes_collections(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        log.log("AESO", "prices", _utc(2024, 2, 1), _utc(2024, 2, 29), 696, "gridstatus_aeso")
        result = log.status()
        assert len(result) == 1
        row = result.iloc[0]
        assert row["market"] == "AESO"
        assert row["data_type"] == "prices"
        assert row["total_rows"] == 1440

    def test_multiple_markets(self, tmp_path):
        log = CollectionLog(data_dir=tmp_path)
        log.log("AESO", "prices", _utc(2024, 1, 1), _utc(2024, 1, 31), 744, "gridstatus_aeso")
        log.log("GB", "demand", _utc(2024, 1, 1), _utc(2024, 1, 31), 1488, "bmrs")
        result = log.status()
        assert len(result) == 2
