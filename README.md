# Open Electricity Data Toolkit

A Python library for collecting, storing, and harmonizing electricity market data across North American and European jurisdictions. The main contribution is the harmonization layer — normalizing data from different ISOs and TSOs into a common schema so cross-market analysis is straightforward — plus scheduled collection with local storage to overcome API lookback limitations.

## Quick start (MVP)

The current release supports **AESO** (Alberta) and **IESO** (Ontario) via the `gridstatus` library. European markets, resampling, and scheduled collection are on the roadmap.

```bash
pip install -e ".[viz]"
```

```python
from elec_data import Toolkit

tk = Toolkit(data_dir="./data")

# Collect and query AESO prices
tk.collect(["AESO"], ["prices"], "2024-01-01", "2024-12-31")
prices = tk.get_prices(markets=["AESO"], start="2024-01-01", end="2024-12-31")
print(prices.head())

# Demand and generation
tk.collect(["AESO"], ["demand", "generation"], "2024-06-01", "2024-06-30")
demand = tk.get_demand(markets=["AESO"], start="2024-06-01", end="2024-06-30")
gen = tk.get_generation(markets=["AESO"], start="2024-06-01", end="2024-06-30")

# What data is in the local store?
tk.status()
```

See [`examples/01_quickstart.ipynb`](examples/01_quickstart.ipynb) for a walkthrough with plots.

> **Note:** AESO requires an API key (`AESO_API_KEY` environment variable).
> IESO does not require a key.

## Why this exists

Electricity market data has three problems:

1. **Lookback windows expire.** Many ISOs only serve 1-2 years of historical data via API. Miss it and it's gone. This toolkit schedules regular pulls and archives everything locally in Parquet so you build up history over time.

2. **Every market is different.** Alberta reports hourly with a single provincial price. Ontario has hourly HOEP plus monthly Global Adjustment. GB settles in 30-minute periods. Germany uses 15-minute intervals. This toolkit normalizes everything into a common schema so you can query multiple markets at once and get back a clean, aligned DataFrame.

3. **Scattered Python libraries.** `gridstatus` covers North American ISOs. `entsoe-py` covers European markets. Elexon has its own API, OMIE has its own. This toolkit wraps them into one interface and handles the idiosyncrasies of each source.

The approach: use `gridstatus` and `entsoe-py` as dependencies for data fetching (no point rewriting their API clients), and focus our original code on harmonization, storage, scheduled collection, and the cross-market query interface.

## Architecture

```
User / Notebook
  toolkit.get_prices(["AESO","GB"], start, end)
        |
  Query Interface
  Checks local Parquet first, fetches only missing data from APIs
        |
  +-----------+  +-----------+  +----------+
  | Local     |  | Harmonize |  | Market   |
  | Store     |  | (schema   |  | Metadata |
  | (Parquet  |  |  mapping,  |  | Registry |
  |  + DuckDB)|  |  resample, |  |          |
  |           |  |  timezone) |  |          |
  +-----------+  +-----+-----+  +----------+
                       |
                 Data Collectors
                 - gridstatus (AESO, IESO, US ISOs)
                 - entsoe-py (European bidding zones)
                 - Custom clients (Elexon BMRS, AESO supplemental, IESO supplemental)
```

## Data sources

### North America (via gridstatus)

| Market | Coverage | Gaps needing custom clients |
|--------|----------|---------------------------|
| Alberta (AESO) | Pool price, load, fuel mix, interchange | Merit order snapshots (2-month lag from ETS), CSD unit-level generation, ancillary services |
| Ontario (IESO) | HOEP, load, fuel mix, interchange | Global Adjustment (monthly, separate page), generator output XML reports, capacity auction results |
| US (CAISO, ERCOT, PJM, NYISO, MISO, SPP, ISONE) | Full coverage via gridstatus | Not primary focus but available |

### Europe (via entsoe-py + custom clients)

| Market | Source | Coverage | Gaps |
|--------|--------|----------|------|
| Pan-European | ENTSO-E Transparency Platform | Day-ahead prices, generation by type, load, cross-border flows | Data quality varies by country; some generation classifications inconsistent |
| Great Britain | Elexon BMRS | System prices (SSP/SBP), generation by fuel, demand | BM unit-level data needs careful pagination; must use new Insights Solution API |
| Iberian Peninsula | OMIE + ENTSO-E | Day-ahead prices, generation | Intraday session data may need OMIE direct |

### Weather and supplemental

| Source | Data | Access |
|--------|------|--------|
| ERA5 (Copernicus) | Wind speed, solar irradiance, temperature | Free API, large downloads |
| Environment Canada | Station-level temperature, wind | Free CSV |

## Data availability and lookback windows

This is the core problem the toolkit solves. Each source has different lookback, and if you don't archive before it expires, the data is lost.

| Source | Lookback | Urgency |
|--------|----------|---------|
| AESO (gridstatus) | Price/demand back to ~2000 via ETS reports. CSD generation back to ~2010. | Low for prices. Medium for generation — collect regularly. |
| IESO (gridstatus) | HOEP back to 2002. Generator output XML: rolling ~3 years. | Low for prices. High for generator output — archive regularly. |
| ENTSO-E (entsoe-py) | 2015 onwards only (Regulation 543/2013). No pre-2015 from this API. | High — start collecting now. |
| Elexon BMRS | Most data back to ~2001. No explicit lookback limit. | Low for archival. Medium for BM unit-level (large volumes). |
| OMIE | Day-ahead prices back to ~1998 via file downloads. | Low — good historical coverage. |

## Granularity harmonization

Markets report at different time resolutions, which is a real problem for cross-market analysis.

| Market | Price | Demand | Generation |
|--------|-------|--------|-----------|
| AESO | 1-min (SMP) / hourly (pool price) | 1-min / hourly | 5-min / hourly |
| IESO | Hourly (HOEP) | Hourly | Hourly |
| ENTSO-E (most EU) | 60-min or 15-min | 15-min or 60-min | 15-min or 60-min |
| GB (Elexon) | 30-min | 30-min | 30-min |
| OMIE (Spain) | 60-min | 60-min | 60-min |

The toolkit stores data at native resolution and resamples on query. This preserves maximum information while making cross-market comparison easy.

```python
# Stored at native resolution per market
# Queried at whatever resolution you need
prices = toolkit.get_prices(
    markets=["AESO", "IESO", "GB", "DE_LU"],
    start="2024-01-01",
    end="2024-12-31",
    resolution="hourly"
)
```

### Resampling rules

| Data type | Downsample (e.g. 5-min to hourly) | Upsample (e.g. hourly to 15-min) |
|-----------|-----------------------------------|----------------------------------|
| Prices | Mean (or VWAP where volume available) | Forward-fill |
| Demand | Mean | Linear interpolation |
| Generation | Mean | Linear interpolation |
| Flows | Mean | Linear interpolation |

### Timezone handling

All data stored internally as UTC. The query interface accepts and returns UTC by default, with an optional `tz` parameter for local time display.

| Market | Timezone | DST? |
|--------|----------|------|
| AESO | America/Edmonton | Yes |
| IESO | America/Toronto | Yes |
| GB | Europe/London | Yes |
| Germany | Europe/Berlin | Yes |
| Spain | Europe/Madrid | Yes |

DST edge cases the harmonizer handles: spring forward (missing hour — no row in local time, continuous in UTC), fall back (duplicated hour — deduplicate, keep first).

## Storage

Parquet + DuckDB, not a server database. Zero cost, zero maintenance, portable, git-friendly.

```
data/
  raw/
    aeso/
      prices/         # 2020.parquet, 2021.parquet, ...
      demand/
      generation/
      merit_order/
      interchange/
    ieso/
      prices/         # HOEP
      demand/
      generation/
      global_adjustment/
      intertie/
    entsoe/
      de/             # Germany-Luxembourg
        prices/
        demand/
        generation/
      fr/
      es/
      gb/
      cross_border_flows/
    bmrs/             # GB Elexon data
      system_prices/
      generation/
      demand/
  metadata/
    collection_log.parquet
    data_quality_log.parquet
  catalog.duckdb      # Query layer over Parquet, not a copy of data
```

Small/medium data (prices, demand, aggregate generation) is committed to the repo — a few hundred KB per year per market as Parquet. Large data (unit-level generation, BM-level, weather) is gitignored with download scripts to recreate from APIs.

## Common data model

All data gets normalized to one of these schemas regardless of source.

### Prices

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Start of delivery period |
| market | string | e.g. "AESO", "GB", "DE_LU" |
| price | float64 | Local currency per MWh |
| currency | string | ISO 4217 (CAD, EUR, GBP) |
| price_type | string | day_ahead, real_time, balancing_buy, balancing_sell, pool |
| resolution_minutes | int | Native resolution of this row |
| source | string | e.g. "gridstatus_aeso", "entsoe" |

### Demand

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Period start |
| market | string | Market identifier |
| demand_mw | float64 | System demand in MW |
| demand_type | string | actual, forecast_day_ahead, forecast_intraday |
| resolution_minutes | int | Native resolution |
| source | string | Data source |

### Generation

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Period start |
| market | string | Market identifier |
| fuel_type | string | Harmonized fuel type (see mapping below) |
| generation_mw | float64 | Average MW over the period |
| resolution_minutes | int | Native resolution |
| source | string | Data source |

### Cross-border flows

| Column | Type | Description |
|--------|------|-------------|
| timestamp_utc | datetime64[ns, UTC] | Period start |
| from_market | string | Exporting market |
| to_market | string | Importing market |
| flow_mw | float64 | Physical flow in MW |
| resolution_minutes | int | Native resolution |
| source | string | Data source |

### Fuel type mapping

| Standard | AESO | IESO | ENTSO-E | Elexon |
|----------|------|------|---------|--------|
| coal | COAL | — | Fossil Hard coal, Lignite | COAL |
| gas | GAS | GAS | Fossil Gas | CCGT, OCGT |
| nuclear | — | NUCLEAR | Nuclear | NUCLEAR |
| hydro | HYDRO | HYDRO | Hydro (all types) | PS, NPSHYD |
| wind | WIND | WIND | Wind Onshore/Offshore | WIND |
| solar | SOLAR | SOLAR | Solar | SOLAR |
| biomass | BIOMASS | BIOMASS | Biomass | BIOMASS |
| other | DUAL_FUEL, etc. | — | Fossil Oil, Other, Waste | OIL, OTHER |
| storage | ENERGY_STORAGE | — | Pumped storage (consuming) | PS (consuming) |

This mapping will need refinement through testing. The ENTSO-E "Other" category is known for containing misclassified gas generation.

## Query interface

```python
from elec_data import Toolkit

tk = Toolkit(data_dir="./data")

# Prices across markets, resampled to hourly
prices = tk.get_prices(
    markets=["AESO", "IESO", "GB", "DE_LU", "ES"],
    start="2023-01-01",
    end="2024-01-01",
    resolution="hourly",
    pivot=True               # Wide format: columns = markets
)

# Generation mix for one market
gen = tk.get_generation(
    markets=["AESO"],
    start="2024-06-01",
    end="2024-06-30",
    fuel_types=["gas", "wind", "solar", "coal"]
)

# Cross-border flows
flows = tk.get_flows(from_market="FR", to_market="ES", start="2023-01-01", end="2024-01-01")

# What data do I have?
tk.status()
```

The query interface checks local storage first and only hits APIs for missing data.

## Scheduled collection

The scheduler runs daily, pulls latest data from each source, appends to the Parquet store, and logs what it did. Backfills are resumable — if it fails halfway through, re-running picks up where it left off.

```bash
python -m elec_data.scheduler run               # All configured markets
python -m elec_data.scheduler run --market aeso  # Just AESO
python -m elec_data.scheduler backfill --market aeso --start 2020-01-01 --end 2024-12-31
python -m elec_data.scheduler status             # What's been collected
```

For automation, a GitHub Actions workflow runs daily, collects new data, and commits the updated Parquet files back to the repo.

## Market metadata registry

Structured reference data about each market lives in `src/elec_data/registry/market_registry.json`. Covers timezone, currency, native resolution, data sources, price caps, interconnections, and notes about market structure. Currently includes AESO, IESO, DE_LU, GB, and ES.

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
      toolkit.py              # User-facing query interface
      collectors/
        base.py               # Abstract base collector
        gridstatus_collector.py
        entsoe_collector.py
        bmrs_collector.py
        aeso_supplemental.py
        ieso_supplemental.py
      harmonize/
        schemas.py            # Pydantic models for common data schemas
        fuel_mapping.py
        resampler.py
        timezone.py
        currency.py
      storage/
        parquet_store.py
        collection_log.py
        quality_checks.py
        catalog.py
      scheduler/
        runner.py
        jobs.py
        backfill.py
      registry/
        markets.py
        market_registry.json
  data/
  tests/
  examples/
  .github/workflows/
```

## Implementation phases

### Phase 1: Core infrastructure + AESO

Get the pipeline working end-to-end for a single market: collect, store, query, return DataFrame. Package skeleton, Pydantic schemas, Parquet storage, collection log, market registry, gridstatus collector for AESO, basic Toolkit class, tests, quickstart notebook.

Milestone: `tk.get_prices(markets=["AESO"], start="2024-01-01", end="2024-12-31")` works.

### Phase 2: Multi-market + harmonization

Add IESO and European markets (ENTSO-E, Elexon BMRS). Implement resampling, timezone handling, fuel type mapping. Extend Toolkit for multi-market queries with resolution alignment.

Milestone: `tk.get_prices(markets=["AESO","IESO","GB","DE_LU","ES"], resolution="hourly", pivot=True)` works.

### Phase 3: Scheduling + supplemental data

Automated daily collection via GitHub Actions. Supplemental collectors for AESO merit order, IESO Global Adjustment. Cross-border flows. Data quality checks. DuckDB catalog. Resumable backfill.

### Phase 4: Polish

Currency conversion, comprehensive test suite (>80% coverage), full documentation, CI/CD (ruff, black, mypy, pytest), status dashboard notebook.

## Dependencies

Core: `gridstatus>=0.29`, `pandas>=2.2`, `pyarrow>=15.0`, `duckdb>=1.0`, `python-dotenv>=1.0`, `tqdm>=4.64`, `pydantic>=2.5`

Dev: `pytest`, `pytest-cov`, `ruff`, `black`, `mypy`

Viz: `plotly>=5.0`, `jupyter`

## API keys

| Source | Required? | Registration |
|--------|-----------|-------------|
| gridstatus | No | — |
| ENTSO-E | Yes (free) | https://transparency.entsoe.eu |
| Elexon BMRS | Yes (free) | https://bmrs.elexon.co.uk |
| ERA5 | Yes (free) | https://cds.climate.copernicus.eu |

## Design decisions

| Decision | Why |
|----------|-----|
| Use gridstatus/entsoe-py as deps | They already work. Our value-add is harmonization and storage, not API scraping. |
| Store at native resolution | Downsampling destroys information. Store native, resample on read. |
| Parquet over CSV | 10-30x smaller, type preservation, partition-friendly, native DuckDB support. |
| DuckDB over SQLite | DuckDB queries Parquet directly. SQLite is row-oriented and can't. |
| UTC internally | Eliminates DST ambiguity. Cross-market alignment is trivial. |
| GitHub Actions for scheduling | Free for public repos, transparent, no cloud account needed. |
| Collection log as Parquet | Query "when was my data last updated?" with the same tools as the data itself. |
| Fuel mapping as explicit table | Easy to modify or extend for different groupings (thermal, clean, etc.). |
| Market registry as JSON | Human-readable, easy to extend, doubles as documentation. |
