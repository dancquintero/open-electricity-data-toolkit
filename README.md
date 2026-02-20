# Open Electricity Data Toolkit

A Python library for collecting, storing, and harmonizing electricity market data across North American jurisdictions. The toolkit normalizes data from different ISOs into a common schema, stores it locally as Parquet, and provides a single query interface that checks the local archive before fetching from upstream APIs.

**Current status:** MVP release supporting AESO (Alberta) and IESO (Ontario) via the [gridstatus](https://github.com/kmax12/gridstatus) library. European markets and additional features are on the [roadmap](#roadmap).

## Quick start

```bash
pip install -e ".[viz]"
```

```python
from elec_data import Toolkit

tk = Toolkit(data_dir="./data")

# Collect AESO prices for a date range
tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-12-31")

# Query stored data (returns a pandas DataFrame)
prices = tk.get_prices(markets=["AESO"], start="2024-01-01", end="2024-12-31")

# Demand and generation follow the same pattern
tk.collect(["AESO"], ["demand", "generation"], "2024-06-01", "2024-06-30")
demand = tk.get_demand(markets=["AESO"], start="2024-06-01", end="2024-06-30")
gen = tk.get_generation(markets=["AESO"], start="2024-06-01", end="2024-06-30")

# View a summary of locally stored data
tk.status()
```

See [`examples/01_quickstart.ipynb`](examples/01_quickstart.ipynb) for a walkthrough with plots.

> **Note:** AESO requires a free API key set as `AESO_API_KEY` in your environment.
> IESO does not require a key.

## Motivation

Electricity market data presents several challenges for cross-market analysis:

1. **Limited lookback windows.** Many ISOs expose only 1--2 years of historical data through their APIs. Without regular archival, older data becomes permanently unavailable. This toolkit collects data on a recurring basis and stores it locally in Parquet so that historical coverage grows over time.

2. **Inconsistent schemas across markets.** Alberta reports a single hourly pool price. Ontario publishes the Hourly Ontario Energy Price (HOEP). European markets use 15- or 30-minute settlement periods with different price types. Each source returns data in its own format, column naming convention, and timezone. This toolkit normalizes all of it into a common schema so that multi-market queries return clean, aligned DataFrames.

3. **Fragmented tooling.** `gridstatus` covers North American ISOs; `entsoe-py` covers European markets; Elexon and OMIE each have their own APIs. Rather than reimplementing these clients, this toolkit wraps them behind a unified interface and focuses on harmonization, storage, and the query layer.

## Architecture

```
User / Notebook
    tk.get_prices(["AESO", "IESO"], start, end)
          |
    Toolkit (query interface)
    Reads from local Parquet; fetches only missing data from APIs
          |
    +-------------+  +-------------+  +-----------+
    | ParquetStore|  | Harmonize   |  | Market    |
    | (year-      |  | (schemas,   |  | Registry  |
    |  partitioned|  |  fuel map,  |  | (JSON)    |
    |  Parquet)   |  |  timezone)  |  |           |
    +-------------+  +------+------+  +-----------+
                            |
                    Data Collectors
                    - GridstatusCollector (AESO, IESO)
```

## Supported markets

| Market | ISO | Data types | Resolution | Source |
|--------|-----|------------|------------|--------|
| Alberta | AESO | Prices (pool), demand, generation by fuel | Hourly | gridstatus |
| Ontario | IESO | Prices (HOEP), demand, generation by fuel | Hourly | gridstatus |

## Common data model

All data is normalized to one of the following schemas regardless of source market.

### Prices

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Start of delivery period |
| market | string | Market identifier (e.g. "AESO", "IESO") |
| price | float64 | Energy price in local currency per MWh |
| currency | string | ISO 4217 currency code (CAD, EUR, GBP) |
| price_type | string | day_ahead, real_time, balancing_buy, balancing_sell, pool |
| resolution_minutes | int | Native time resolution of this observation |
| source | string | Data source identifier (e.g. "gridstatus_aeso") |

### Demand

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Period start |
| market | string | Market identifier |
| demand_mw | float64 | System demand in MW |
| demand_type | string | actual, forecast_day_ahead, forecast_intraday |
| resolution_minutes | int | Native time resolution |
| source | string | Data source identifier |

### Generation

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Period start |
| market | string | Market identifier |
| fuel_type | string | Harmonized fuel type (see mapping below) |
| generation_mw | float64 | Average generation in MW over the period |
| resolution_minutes | int | Native time resolution |
| source | string | Data source identifier |

### Fuel type mapping

| Standard | AESO | IESO |
|----------|------|------|
| coal | Coal | -- |
| gas | Cogeneration, Combined Cycle, Gas Fired Steam, Simple Cycle | Gas |
| nuclear | -- | Nuclear |
| hydro | Hydro | Hydro |
| wind | Wind | Wind |
| solar | Solar | Solar |
| biomass | -- | Biofuel |
| storage | Energy Storage | -- |
| other | Other | Other |

## Storage

Data is stored locally as year-partitioned Parquet files. All timestamps are stored in UTC.

```
data/
  raw/
    aeso/
      prices/         # 2023.parquet, 2024.parquet, ...
      demand/
      generation/
    ieso/
      prices/
      demand/
      generation/
  metadata/
    collection_log.parquet
```

Parquet provides efficient columnar storage (10--30x smaller than CSV), preserves data types, and can be queried directly with DuckDB or pandas. Small and medium datasets (prices, demand, aggregate generation) are a few hundred KB per market-year and can be committed to the repository so that anyone cloning the project has data immediately.

## Project structure

```
open-electricity-data-toolkit/
  README.md
  pyproject.toml
  LICENSE
  .gitignore
  .env.example
  src/
    elec_data/
      __init__.py
      toolkit.py                  # User-facing query interface
      collectors/
        base.py                   # Abstract base collector
        gridstatus_collector.py   # AESO + IESO via gridstatus
      harmonize/
        schemas.py                # Pydantic models for common data schemas
      storage/
        parquet_store.py          # Year-partitioned Parquet read/write
        collection_log.py         # Tracks what has been collected
      registry/
        markets.py                # MarketRegistry class
        market_registry.json      # Per-market metadata (timezone, currency, etc.)
  data/
  tests/
  examples/
```

## Roadmap

### Phase 2: Multi-market and harmonization

- Add European markets via `entsoe-py` (Germany, France, Spain, Great Britain) and Elexon BMRS for GB settlement data.
- Implement time-resolution resampling so that markets with different native intervals (15-min, 30-min, hourly) can be queried at a common resolution.
- Add cross-border flow data and pivot/wide-format output for multi-market price comparison.

### Phase 3: Scheduling and supplemental data

- Automated daily collection via GitHub Actions with resumable backfill.
- Supplemental collectors for data not available through gridstatus (AESO merit order snapshots, IESO Global Adjustment).
- Data quality checks and a DuckDB catalog layer for SQL-based querying over the Parquet store.

### Phase 4: Polish

- Currency conversion for cross-market price comparison.
- Comprehensive test suite (>80% coverage) and CI/CD pipeline.
- Documentation and a data-coverage dashboard notebook.

## Dependencies

**Core:** `gridstatus`, `pandas`, `pyarrow`, `duckdb`, `python-dotenv`, `tqdm`, `pydantic`

**Dev:** `pytest`, `pytest-cov`, `ruff`, `black`, `mypy`

**Visualization (optional):** `plotly`, `jupyter`

## API keys

| Source | Required | Registration |
|--------|----------|--------------|
| AESO (via gridstatus) | Yes (free) | Set `AESO_API_KEY` in `.env` |
| IESO (via gridstatus) | No | -- |
| ENTSO-E | Yes (free) | https://transparency.entsoe.eu (planned) |
| Elexon BMRS | Yes (free) | https://bmrs.elexon.co.uk (planned) |

## Design decisions

| Decision | Rationale |
|----------|-----------|
| Use gridstatus/entsoe-py as dependencies | These libraries already handle API specifics. The toolkit's value is in harmonization, storage, and the unified query interface. |
| Store at native resolution | Downsampling discards information. Data is stored at its original granularity and resampled on read. |
| Parquet over CSV | 10--30x smaller file sizes, native type preservation, partition-friendly, and directly queryable with DuckDB. |
| UTC internally | Eliminates daylight saving time ambiguity and makes cross-market timestamp alignment straightforward. |
| Collection log as Parquet | Collection metadata can be queried with the same tools as the data itself. |
| Market registry as JSON | Human-readable, easy to extend, and serves as inline documentation of each market's conventions. |
