"""Tests for elec_data.harmonize.schemas."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest
from pydantic import ValidationError

from elec_data.harmonize.schemas import (
    CollectionLogEntry,
    DataRequest,
    DemandRecord,
    DemandType,
    FlowRecord,
    FuelType,
    GenerationRecord,
    PriceRecord,
    PriceType,
    SCHEMA_MAP,
    VALID_DATA_TYPES,
    VALID_MARKETS,
    validate_dataframe,
)


# ---- Enums ----


class TestPriceType:
    def test_has_five_members(self):
        assert len(PriceType) == 5

    def test_string_equality(self):
        assert PriceType.DAY_AHEAD == "day_ahead"
        assert PriceType.REAL_TIME == "real_time"
        assert PriceType.POOL == "pool"

    def test_is_str_subclass(self):
        assert isinstance(PriceType.POOL, str)


class TestDemandType:
    def test_has_three_members(self):
        assert len(DemandType) == 3

    def test_string_equality(self):
        assert DemandType.ACTUAL == "actual"
        assert DemandType.FORECAST_DAY_AHEAD == "forecast_day_ahead"


class TestFuelType:
    def test_has_ten_members(self):
        assert len(FuelType) == 10

    def test_string_equality(self):
        assert FuelType.COAL == "coal"
        assert FuelType.STORAGE == "storage"


# ---- Constants ----


class TestConstants:
    def test_valid_markets(self):
        assert VALID_MARKETS == {"AESO", "IESO", "DE_LU", "GB", "ES"}

    def test_valid_data_types(self):
        assert VALID_DATA_TYPES == {"prices", "demand", "generation", "flows"}

    def test_schema_map_keys(self):
        assert set(SCHEMA_MAP.keys()) == VALID_DATA_TYPES

    def test_schema_map_values(self):
        assert SCHEMA_MAP["prices"] is PriceRecord
        assert SCHEMA_MAP["demand"] is DemandRecord
        assert SCHEMA_MAP["generation"] is GenerationRecord
        assert SCHEMA_MAP["flows"] is FlowRecord


# ---- PriceRecord ----


class TestPriceRecord:
    def test_valid(self):
        rec = PriceRecord(
            timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
            market="AESO",
            price=45.50,
            currency="CAD",
            price_type=PriceType.POOL,
            resolution_minutes=60,
            source="gridstatus_aeso",
        )
        assert rec.price == 45.50
        assert rec.market == "AESO"

    def test_missing_field_raises(self):
        with pytest.raises(ValidationError):
            PriceRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                market="AESO",
                # price missing
                currency="CAD",
                price_type=PriceType.POOL,
                resolution_minutes=60,
                source="gridstatus_aeso",
            )

    def test_resolution_zero_raises(self):
        with pytest.raises(ValidationError):
            PriceRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                market="AESO",
                price=45.50,
                currency="CAD",
                price_type=PriceType.POOL,
                resolution_minutes=0,
                source="gridstatus_aeso",
            )

    def test_resolution_negative_raises(self):
        with pytest.raises(ValidationError):
            PriceRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                market="AESO",
                price=45.50,
                currency="CAD",
                price_type=PriceType.POOL,
                resolution_minutes=-5,
                source="gridstatus_aeso",
            )


# ---- DemandRecord ----


class TestDemandRecord:
    def test_valid(self):
        rec = DemandRecord(
            timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
            market="AESO",
            demand_mw=9500.0,
            demand_type=DemandType.ACTUAL,
            resolution_minutes=60,
            source="gridstatus_aeso",
        )
        assert rec.demand_mw == 9500.0

    def test_missing_field_raises(self):
        with pytest.raises(ValidationError):
            DemandRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                market="AESO",
                # demand_mw missing
                demand_type=DemandType.ACTUAL,
                resolution_minutes=60,
                source="gridstatus_aeso",
            )


# ---- GenerationRecord ----


class TestGenerationRecord:
    def test_valid(self):
        rec = GenerationRecord(
            timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
            market="AESO",
            fuel_type=FuelType.GAS,
            generation_mw=5000.0,
            resolution_minutes=60,
            source="gridstatus_aeso",
        )
        assert rec.fuel_type == FuelType.GAS

    def test_missing_field_raises(self):
        with pytest.raises(ValidationError):
            GenerationRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                market="AESO",
                # fuel_type missing
                generation_mw=5000.0,
                resolution_minutes=60,
                source="gridstatus_aeso",
            )


# ---- FlowRecord ----


class TestFlowRecord:
    def test_valid(self):
        rec = FlowRecord(
            timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
            from_market="AESO",
            to_market="BC",
            flow_mw=200.0,
            resolution_minutes=60,
            source="gridstatus_aeso",
        )
        assert rec.flow_mw == 200.0

    def test_missing_field_raises(self):
        with pytest.raises(ValidationError):
            FlowRecord(
                timestamp_utc=datetime(2024, 1, 1, tzinfo=timezone.utc),
                from_market="AESO",
                # to_market missing
                flow_mw=200.0,
                resolution_minutes=60,
                source="gridstatus_aeso",
            )


# ---- CollectionLogEntry ----


class TestCollectionLogEntry:
    def test_valid(self):
        entry = CollectionLogEntry(
            market="AESO",
            data_type="prices",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
            rows_collected=744,
            collected_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
            source="gridstatus_aeso",
        )
        assert entry.status == "success"

    def test_negative_rows_raises(self):
        with pytest.raises(ValidationError):
            CollectionLogEntry(
                market="AESO",
                data_type="prices",
                start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
                rows_collected=-1,
                collected_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
                source="gridstatus_aeso",
            )

    def test_zero_rows_allowed(self):
        entry = CollectionLogEntry(
            market="AESO",
            data_type="prices",
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
            rows_collected=0,
            collected_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
            source="gridstatus_aeso",
        )
        assert entry.rows_collected == 0


# ---- DataRequest ----


class TestDataRequest:
    def test_valid(self):
        req = DataRequest(
            markets=["AESO", "GB"],
            start="2024-01-01",
            end="2024-12-31",
            data_type="prices",
        )
        assert req.resolution is None

    def test_with_resolution(self):
        req = DataRequest(
            markets=["AESO"],
            start="2024-01-01",
            end="2024-12-31",
            data_type="demand",
            resolution="hourly",
        )
        assert req.resolution == "hourly"


# ---- validate_dataframe ----


class TestValidateDataframe:
    def test_valid_returns_empty(self, sample_price_df):
        errors = validate_dataframe(sample_price_df, PriceRecord)
        assert errors == []

    def test_missing_one_column(self):
        df = pd.DataFrame({
            "timestamp_utc": [],
            "market": [],
            "price": [],
            "currency": [],
            "price_type": [],
            "source": [],
            # resolution_minutes missing
        })
        errors = validate_dataframe(df, PriceRecord)
        assert len(errors) == 1
        assert "resolution_minutes" in errors[0]

    def test_missing_multiple_columns(self):
        df = pd.DataFrame({"timestamp_utc": [], "market": []})
        errors = validate_dataframe(df, PriceRecord)
        assert len(errors) == 5

    def test_empty_df_correct_columns(self, sample_price_df):
        empty = sample_price_df.iloc[:0]
        errors = validate_dataframe(empty, PriceRecord)
        assert errors == []

    def test_error_contains_schema_name(self):
        df = pd.DataFrame({"timestamp_utc": []})
        errors = validate_dataframe(df, PriceRecord)
        assert all("PriceRecord" in e for e in errors)

    def test_all_record_types(
        self, sample_price_df, sample_demand_df, sample_generation_df, sample_flow_df
    ):
        assert validate_dataframe(sample_price_df, PriceRecord) == []
        assert validate_dataframe(sample_demand_df, DemandRecord) == []
        assert validate_dataframe(sample_generation_df, GenerationRecord) == []
        assert validate_dataframe(sample_flow_df, FlowRecord) == []
