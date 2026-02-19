"""Common data model schemas for electricity market data.

Defines Pydantic models documenting the expected DataFrame column schemas.
These are used for validation and documentation, not for row-by-row
construction of DataFrames. Data flows as pandas DataFrames with these
column names; the models here are for spot-check validation and as a
single source of truth for the expected schema.
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum

import pandas as pd
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums for constrained string fields
# ---------------------------------------------------------------------------


class PriceType(str, Enum):
    """Valid price type identifiers."""

    DAY_AHEAD = "day_ahead"
    REAL_TIME = "real_time"
    BALANCING_BUY = "balancing_buy"
    BALANCING_SELL = "balancing_sell"
    POOL = "pool"


class DemandType(str, Enum):
    """Valid demand type identifiers."""

    ACTUAL = "actual"
    FORECAST_DAY_AHEAD = "forecast_day_ahead"
    FORECAST_INTRADAY = "forecast_intraday"


class FuelType(str, Enum):
    """Harmonized fuel type identifiers used across all markets."""

    COAL = "coal"
    GAS = "gas"
    NUCLEAR = "nuclear"
    HYDRO = "hydro"
    WIND = "wind"
    SOLAR = "solar"
    BIOMASS = "biomass"
    OTHER_RENEWABLE = "other_renewable"
    OTHER = "other"
    STORAGE = "storage"


# Markets currently in the registry
VALID_MARKETS = {"AESO", "IESO", "DE_LU", "GB", "ES"}

# Data types the toolkit handles
VALID_DATA_TYPES = {"prices", "demand", "generation", "flows"}


# ---------------------------------------------------------------------------
# Data record schemas — one per data type
# ---------------------------------------------------------------------------


class PriceRecord(BaseModel):
    """Schema for a single price observation.

    Parameters
    ----------
    timestamp_utc : datetime
        Start of the delivery period, UTC-aware.
    market : str
        Market identifier (e.g. "AESO", "GB").
    price : float
        Energy price in local currency per MWh.
    currency : str
        ISO 4217 currency code (CAD, EUR, GBP, USD).
    price_type : PriceType
        Type of price signal.
    resolution_minutes : int
        Native time resolution of this observation in minutes.
    source : str
        Data source identifier (e.g. "gridstatus_aeso", "entsoe").
    """

    timestamp_utc: datetime
    market: str
    price: float
    currency: str
    price_type: PriceType
    resolution_minutes: int = Field(gt=0)
    source: str


class DemandRecord(BaseModel):
    """Schema for a single demand observation.

    Parameters
    ----------
    timestamp_utc : datetime
        Period start, UTC-aware.
    market : str
        Market identifier.
    demand_mw : float
        System demand in MW.
    demand_type : DemandType
        Whether this is actual or a forecast.
    resolution_minutes : int
        Native time resolution in minutes.
    source : str
        Data source identifier.
    """

    timestamp_utc: datetime
    market: str
    demand_mw: float
    demand_type: DemandType
    resolution_minutes: int = Field(gt=0)
    source: str


class GenerationRecord(BaseModel):
    """Schema for a single generation observation.

    Parameters
    ----------
    timestamp_utc : datetime
        Period start, UTC-aware.
    market : str
        Market identifier.
    fuel_type : FuelType
        Harmonized fuel type.
    generation_mw : float
        Average generation in MW over the period.
    resolution_minutes : int
        Native time resolution in minutes.
    source : str
        Data source identifier.
    """

    timestamp_utc: datetime
    market: str
    fuel_type: FuelType
    generation_mw: float
    resolution_minutes: int = Field(gt=0)
    source: str


class FlowRecord(BaseModel):
    """Schema for a single cross-border flow observation.

    Parameters
    ----------
    timestamp_utc : datetime
        Period start, UTC-aware.
    from_market : str
        Exporting market identifier.
    to_market : str
        Importing market identifier.
    flow_mw : float
        Physical flow in MW (positive = from_market to to_market).
    resolution_minutes : int
        Native time resolution in minutes.
    source : str
        Data source identifier.
    """

    timestamp_utc: datetime
    from_market: str
    to_market: str
    flow_mw: float
    resolution_minutes: int = Field(gt=0)
    source: str


# ---------------------------------------------------------------------------
# Request and logging schemas
# ---------------------------------------------------------------------------


class DataRequest(BaseModel):
    """Describes a user query for data.

    Parameters
    ----------
    markets : list[str]
        Market identifiers to query.
    start : str
        Start date as ISO string (e.g. "2024-01-01").
    end : str
        End date as ISO string.
    data_type : str
        One of "prices", "demand", "generation", "flows".
    resolution : str or None
        Target resolution for resampling (e.g. "hourly", "30min", "15min").
        None means return at native resolution.
    """

    markets: list[str]
    start: str
    end: str
    data_type: str
    resolution: str | None = None


class CollectionLogEntry(BaseModel):
    """Schema for a single entry in the collection log.

    Parameters
    ----------
    market : str
        Market identifier.
    data_type : str
        One of "prices", "demand", "generation", "flows".
    start_date : datetime
        Start of the collected date range.
    end_date : datetime
        End of the collected date range.
    rows_collected : int
        Number of rows fetched.
    collected_at : datetime
        When the collection happened (UTC).
    source : str
        Data source identifier.
    status : str
        "success" or "error".
    """

    market: str
    data_type: str
    start_date: datetime
    end_date: datetime
    rows_collected: int = Field(ge=0)
    collected_at: datetime
    source: str
    status: str = "success"


# ---------------------------------------------------------------------------
# Schema lookup — maps data type name to its record schema
# ---------------------------------------------------------------------------

SCHEMA_MAP: dict[str, type[BaseModel]] = {
    "prices": PriceRecord,
    "demand": DemandRecord,
    "generation": GenerationRecord,
    "flows": FlowRecord,
}


# ---------------------------------------------------------------------------
# DataFrame validation helper
# ---------------------------------------------------------------------------


def validate_dataframe(df: pd.DataFrame, schema_class: type[BaseModel]) -> list[str]:
    """Check that a DataFrame has the columns expected by a schema.

    Does not construct Pydantic model instances for every row — just checks
    that the required columns are present. Returns a list of error messages
    (empty list means valid).

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to validate.
    schema_class : type[BaseModel]
        A Pydantic model class (e.g. PriceRecord) whose fields define the
        expected columns.

    Returns
    -------
    list[str]
        Error messages for missing columns. Empty if all columns are present.

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({"timestamp_utc": [], "market": [], "price": []})
    >>> errors = validate_dataframe(df, PriceRecord)
    >>> len(errors) > 0  # missing currency, price_type, etc.
    True
    """
    expected = set(schema_class.model_fields.keys())
    actual = set(df.columns)
    missing = expected - actual
    errors = []
    for col in sorted(missing):
        errors.append(f"Missing column: '{col}' (expected by {schema_class.__name__})")
    if errors:
        logger.warning(
            "DataFrame validation failed for %s: %d missing column(s)",
            schema_class.__name__,
            len(errors),
        )
    return errors
