# Open Electricity Data Toolkit

A Python library for collecting, storing, harmonizing, and querying electricity market data across North American and European jurisdictions. Built to support a portfolio of electricity market analysis projects.

**This is not a scraping project.** The value-add is the **harmonization layer** — normalizing data from different ISOs/TSOs into a common schema so cross-market analysis is easy — plus **scheduled collection with local storage** to overcome API lookback limitations.

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture Overview](#architecture-overview)
- [Data Sources](#data-sources)
- [Data Availability Constraints](#data-availability-constraints)
- [Granularity Harmonization](#granularity-harmonization)
- [Storage Strategy](#storage-strategy)
- [Scheduled Data Collection](#scheduled-data-collection)
- [Common Data Model](#common-data-model)
- [Query Interface](#query-interface)
- [Market Metadata Registry](#market-metadata-registry)
- [Project Structure](#project-structure)
- [Implementation Phases](#implementation-phases)
- [Technical Requirements](#technical-requirements)
- [Design Decisions & Rationale](#design-decisions--rationale)

---

## Why This Exists

Electricity market data has three problems this toolkit solves:

1. **Lookback windows expire.** Many ISOs only serve 1-2 years of historical data via API. If you don't collect it, it's gone. This toolkit schedules regular data pulls and archives everything locally in Parquet files so you build up a growing historical dataset over time.

2. **Every market is different.** Alberta reports at 1-minute intervals with a single provincial price. Ontario has hourly HOEP plus monthly Global Adjustment. GB settles in 30-minute periods. Europe uses 15-minute or 60-minute intervals depending on the bidding zone. This toolkit normalizes all of it into a common schema so you can write `get_prices(["AESO", "IESO", "GB"], "2023-01-01", "2024-01-01")` and get back a clean, aligned DataFrame.

3. **Scattered Python libraries.** `gridstatus` covers North American ISOs (including AESO and IESO). `entsoe-py` covers European markets. Elexon has its own API. OMIE has its own. This toolkit wraps them all into one unified interface, handles API keys, and manages the idiosyncrasies of each source.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    User / Notebook                          │
│         toolkit.get_prices(["AESO","GB"], start, end)       │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                  Query Interface                            │
│   Checks local Parquet store first, fetches only missing    │
│   data from APIs, applies granularity alignment             │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
┌────────▼──────┐ ┌──────▼───────┐ ┌─────▼──────────┐
│  Local Store  │ │  Harmonizer  │ │  Market        │
│  (Parquet +   │ │  (Schema     │ │  Metadata      │
│   DuckDB)     │ │  mapping,    │ │  Registry      │
│               │ │  resampling, │ │  (timezones,   │
│               │ │  timezone    │ │  fuel types,   │
│               │ │  handling)   │ │  conventions)  │
└───────────────┘ └──────┬───────┘ └────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                   Data Collectors                           │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  gridstatus (open-source Python library, BSD-3)      │   │
│  │  Wraps: AESO, IESO, CAISO, ERCOT, PJM, NYISO,      │   │
│  │         MISO, SPP, ISONE, EIA                        │   │
│  │  Used as: pip dependency, called via Python API      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  entsoe-py (open-source, MIT)                        │   │
│  │  Wraps: ENTSO-E Transparency Platform API            │   │
│  │  Covers: All European bidding zones                  │   │
│  │  Used as: pip dependency, called via Python API      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Custom clients (written by us)                      │   │
│  │  - Elexon BMRS (GB balancing mechanism data)         │   │
│  │  - OMIE (Iberian day-ahead/intraday, if entsoe-py   │   │
│  │    coverage is insufficient)                         │   │
│  │  - AESO supplemental (merit order snapshots, CSD     │   │
│  │    generation data — not in gridstatus)              │   │
│  │  - IESO supplemental (Global Adjustment, generator   │   │
│  │    output by fuel — not in gridstatus)               │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Principle: Don't Reinvent the Wheel

- **If `gridstatus` or `entsoe-py` already fetches the data:** Use them as dependencies. Don't rewrite their API clients.
- **If data exists in those libraries but needs supplementing:** Write thin custom clients only for the gaps (e.g., AESO merit order snapshots, IESO Global Adjustment data, Elexon BOA-level data).
- **Our original code focuses on:** harmonization, storage management, scheduled collection, cross-market query interface, and market metadata.

---

## Data Sources

### North America (via `gridstatus` open-source library)

| Market | ISO | `gridstatus` Support | Key Data Available | Key Gaps (need custom client) |
|--------|-----|---------------------|--------------------|-------------------------------|
| Alberta | AESO | ✅ Pool price, load, fuel mix, interchange | Price, demand, generation by fuel, import/export | Merit order snapshots (2-month lag, CSV from ETS), historical CSD generation data (5-min unit-level), ancillary services prices |
| Ontario | IESO | ✅ HOEP, load, fuel mix, interchange | Price, demand, generation by fuel, inter-tie flows | Global Adjustment (monthly, separate IESO page), generator output and capability (XML reports), capacity auction results |
| US Markets | CAISO, ERCOT, PJM, NYISO, MISO, SPP, ISONE | ✅ Full coverage | LMP/price, load, fuel mix, interchange | Not primary focus but available for comparison projects |
| US Aggregate | EIA | ✅ | National generation, consumption | — |

### Europe (via `entsoe-py` library + custom clients)

| Market | Data Source | Library | Key Data Available | Key Gaps |
|--------|------------|---------|-------------------|----------|
| Pan-European | ENTSO-E Transparency Platform | `entsoe-py` | Day-ahead prices, actual generation by type, load, cross-border flows, installed capacity | Data quality varies by country (see Hirth et al. 2018); some generation type classifications are inconsistent |
| Great Britain | Elexon BMRS | Custom client (or `ElexonDataPortal`) | System prices (SSP/SBP), BOAs, physical notifications, generation by fuel, demand | Full BM unit-level data requires careful API pagination; Insights Solution API is the current version (old API deprecated) |
| Iberian Peninsula | OMIE | `entsoe-py` (for prices, generation) + OMIE direct (for market-specific data) | Day-ahead prices, matched volumes, generation | Intraday session data, Iberian Exception mechanism details may need OMIE direct |
| Carbon | ICE, EEX | Custom client or manual download | EU ETS futures, UK ETS prices | May require manual CSV download from ICE; no free real-time API |

### Weather & Supplemental

| Source | Data | Access |
|--------|------|--------|
| ERA5 (ECMWF Copernicus) | Wind speed, solar irradiance, temperature reanalysis | Free API (CDS API), requires registration, large downloads |
| Environment Canada | Station-level temperature, wind | Free, CSV download |
| AESO Long-Term Outlook | Demand/supply projections, planned generation | PDF/Excel, manual download, updated annually |
| IESO Annual Planning Outlook | Ontario system planning data | PDF/Excel, manual download, updated annually |

---

## Data Availability Constraints

This is the critical problem the toolkit solves. Each data source has different lookback windows, and if you don't archive data before it expires, it's lost.

### Lookback Windows by Source

| Source | API Lookback | Notes | Implication |
|--------|-------------|-------|-------------|
| **AESO (via gridstatus)** | Pool price: back to ~2000 via ETS historical reports. Real-time data: rolling window varies by report type. Merit order snapshots: 2-month publication lag, available historically. CSD generation: monthly updates, historically available back to ~2010. | AESO is relatively generous with historical data. The ETS historical reports page allows downloading price/demand data back to market inception (2000). | **Low urgency** for price/demand archival. **Medium urgency** for merit order and generation data — collect regularly to build continuous series. |
| **IESO (via gridstatus)** | HOEP: available historically back to market opening (2002). Generator output: XML reports available for rolling ~3 years. Adequacy reports: point-in-time snapshots, not versioned. | IESO has good historical price/demand coverage but detailed generator and planning data is more ephemeral. | **Low urgency** for price/demand. **High urgency** for generator output and adequacy data — archive XML reports regularly. |
| **ENTSO-E (via entsoe-py)** | **2015 onwards only** (by regulatory design — Regulation 543/2013 took effect Jan 2015). Some legacy data (2011-2014) available as static file downloads. | This is a hard constraint. You will never get pre-2015 data from this API. For longer time series, you need TSO-specific sources or academic datasets (e.g., Open Power System Data). | **High urgency** — start collecting now. Every day you delay is a day of data you could lose if the platform changes. 2015-present gives ~10 years which is sufficient for most analyses. |
| **Elexon BMRS (GB)** | Most data back to ~2001 (BSC go-live). The new Insights Solution API provides extensive historical access. No explicit lookback limit documented. | GB data is among the most historically complete. However, the API was recently migrated to the "Insights Solution" platform, and the old API is deprecated. Ensure the client targets the new API. | **Low urgency** for archival (deep history available). **Medium urgency** for BM unit-level data — very granular, large volumes, pull incrementally. |
| **OMIE (Iberian)** | Day-ahead prices available historically back to ~1998 via OMIE website file downloads. API coverage varies. | OMIE's website has excellent historical file archives. The ENTSO-E Transparency Platform also covers Iberian prices from 2015+. | **Low urgency** — between OMIE archives and ENTSO-E, historical coverage is good. |
| **gridstatus (US ISOs)** | Varies by ISO. CAISO: ~2018+. ERCOT: ~2018+. PJM: varies by dataset. NYISO: ~2019+. | `gridstatus` pulls directly from ISO websites/APIs, which have varying historical windows. | **Medium urgency** — start collecting to build history beyond what's currently accessible. Not primary focus for this portfolio but worth archiving if running the scheduler anyway. |

### Archival Strategy

```
Priority 1 (start immediately):
  - ENTSO-E: Backfill 2015-present for target bidding zones, then schedule daily pulls
  - IESO: Archive generator output XML reports (rolling 3-year window)
  - AESO: Backfill merit order snapshots and CSD generation data

Priority 2 (after initial backfill):
  - Elexon BMRS: Systematic backfill of BM unit-level data (large volume, do incrementally)
  - US ISOs via gridstatus: Background archival for comparison projects

Priority 3 (periodic manual):
  - Planning outlook documents (AESO LTO, IESO APO, National Grid FES)
  - Carbon price data (EU ETS, UK ETS)
  - Weather data (ERA5 — very large, only download for specific project needs)
```

---

## Granularity Harmonization

Markets report data at different time resolutions. This is a real problem for cross-market analysis.

### Native Granularities by Market

| Market | Price Resolution | Demand Resolution | Generation Resolution |
|--------|-----------------|-------------------|----------------------|
| AESO (Alberta) | 1-minute (SMP), hourly (pool price) | 1-minute actual, hourly summary | 5-minute and hourly (CSD data) |
| IESO (Ontario) | Hourly (HOEP), 5-minute (pre-dispatch) | Hourly | Hourly |
| ERCOT | 5-minute (RT SPP), 15-minute (DA) | 15-minute | Hourly |
| PJM | 5-minute (RT LMP), hourly (DA LMP) | Hourly | Hourly |
| ENTSO-E (most EU) | 60-minute (most), 15-minute (DE, AT, NL, BE) | 15-minute or 60-minute | 15-minute or 60-minute |
| GB (Elexon) | 30-minute (settlement period) | 30-minute | 30-minute |
| OMIE (Spain) | 60-minute | 60-minute | 60-minute |

### Harmonization Approach

The toolkit stores data at **native resolution** and resamples **on query**. This preserves maximum information while making cross-market analysis easy.

```python
# Stored: native resolution per market
# data/aeso/prices/2024.parquet     → 1-min rows (pool price is hourly, SMP is 1-min)
# data/ieso/prices/2024.parquet     → hourly rows
# data/gb/prices/2024.parquet       → 30-min rows
# data/entsoe/de/prices/2024.parquet → 15-min rows

# Queried: user specifies target resolution
prices = toolkit.get_prices(
    markets=["AESO", "IESO", "GB", "DE"],
    start="2024-01-01",
    end="2024-12-31",
    resolution="hourly"  # Resamples to common resolution
)
# Returns: DataFrame with DatetimeIndex (UTC), columns per market
```

### Resampling Rules

Different data types require different aggregation when resampling:

| Data Type | Upsample (e.g., hourly → 15-min) | Downsample (e.g., 5-min → hourly) |
|-----------|----------------------------------|-----------------------------------|
| **Prices** | Forward-fill (price is constant within the settlement period) | Volume-weighted average where volume data available; otherwise simple mean. Also expose: max, min, time-weighted average. |
| **Demand/Load** | Linear interpolation | Mean (represents average power over period) |
| **Generation** | Linear interpolation | Mean |
| **Inter-tie flows** | Linear interpolation | Mean |
| **Cumulative energy (MWh)** | Linear interpolation | Sum (energy is additive) |

### Timezone Handling

All data is stored internally as **UTC**. The query interface accepts and returns timestamps in UTC by default, with an optional `tz` parameter for local time display.

| Market | Native Timezone | UTC Offset | DST? |
|--------|----------------|------------|------|
| AESO | MST (America/Edmonton) | UTC-7 | Yes (MDT = UTC-6) |
| IESO | EST (America/Toronto) | UTC-5 | Yes (EDT = UTC-4) |
| ERCOT | CST (America/Chicago) | UTC-6 | Yes |
| PJM | EST (America/New_York) | UTC-5 | Yes |
| GB | GMT (Europe/London) | UTC+0 | Yes (BST = UTC+1) |
| Germany | CET (Europe/Berlin) | UTC+1 | Yes (CEST = UTC+2) |
| Spain | CET (Europe/Madrid) | UTC+1 | Yes (CEST = UTC+2) |
| France | CET (Europe/Paris) | UTC+1 | Yes (CEST = UTC+2) |

**Critical DST edge cases the harmonizer must handle:**
- **Spring forward:** The missing hour (e.g., 2:00 AM doesn't exist). For hourly data, that row simply doesn't exist in local time. In UTC it's continuous.
- **Fall back:** The repeated hour (e.g., 2:00 AM happens twice). Some ISOs report both; some average them. The harmonizer must detect and handle duplicates.
- **Cross-timezone alignment:** When comparing AESO (MST) with GB (GMT), the "same hour" is offset by 7 hours. UTC storage solves this, but the user must understand that "morning peak" means different clock times.

---

## Storage Strategy

### Why Parquet + DuckDB (Not a Server Database)

| Consideration | Parquet + DuckDB | PostgreSQL / Cloud DB |
|---------------|-----------------|----------------------|
| Cost | $0 | Free tier limited; real cost at scale |
| Setup | `pip install duckdb` | Server install or cloud account |
| Portability | Copy folder, done | Export/import, connection strings |
| Git-friendly | Parquet files can live in repo (up to ~100MB per file) or in `.gitignore` with download scripts | Not git-friendly |
| Query speed | Excellent for analytical queries (columnar) | Good, but overkill for this scale |
| Maintenance | Zero | Backups, updates, connection management |

### Storage Layout

```
data/
├── raw/                              # Native-resolution data, as received from APIs
│   ├── aeso/
│   │   ├── prices/
│   │   │   ├── 2020.parquet          # Hourly pool prices for full year
│   │   │   ├── 2021.parquet
│   │   │   ├── ...
│   │   │   └── 2025.parquet
│   │   ├── demand/
│   │   │   └── ...
│   │   ├── generation/
│   │   │   └── ...                   # By-fuel generation
│   │   ├── merit_order/
│   │   │   └── ...                   # Merit order snapshots (2-month lag)
│   │   └── interchange/
│   │       └── ...                   # BC/Montana/SK imports/exports
│   ├── ieso/
│   │   ├── prices/                   # HOEP
│   │   ├── demand/
│   │   ├── generation/
│   │   ├── global_adjustment/        # Monthly GA data
│   │   └── intertie/                 # QC, MB, NY, MI flows
│   ├── entsoe/
│   │   ├── de/                       # Germany-Luxembourg bidding zone
│   │   │   ├── prices/
│   │   │   ├── demand/
│   │   │   └── generation/
│   │   ├── fr/                       # France
│   │   ├── es/                       # Spain
│   │   ├── gb/                       # GB (ENTSO-E side — supplements Elexon)
│   │   ├── nl/                       # Netherlands
│   │   ├── nordics/                  # Nordic bidding zones (NO1-5, SE1-4, DK1-2, FI)
│   │   └── cross_border_flows/       # Interconnector flow data
│   ├── bmrs/                         # GB Elexon data
│   │   ├── system_prices/            # SSP/SBP
│   │   ├── generation/
│   │   ├── demand/
│   │   └── balancing/                # BOA-level data
│   ├── omie/                         # Iberian-specific (if needed beyond ENTSO-E)
│   ├── carbon/
│   │   ├── eu_ets/                   # EU ETS daily settlement prices
│   │   └── uk_ets/
│   └── us/                           # US ISOs (lower priority, for comparison)
│       ├── ercot/
│       ├── pjm/
│       ├── caiso/
│       └── nyiso/
│
├── metadata/
│   ├── collection_log.parquet        # Tracks what data has been collected and when
│   ├── data_quality_log.parquet      # Tracks anomalies, gaps, quality issues
│   └── market_registry.json          # Market metadata (see Market Metadata Registry)
│
└── catalog.duckdb                    # DuckDB database file — views over Parquet files
                                      # NOT a copy of the data; just a query layer
```

### File Sizing Estimates

| Market | Data Type | Resolution | Rows/Year | Parquet Size/Year |
|--------|-----------|-----------|-----------|-------------------|
| AESO | Pool price | Hourly | 8,760 | ~200 KB |
| AESO | SMP | 1-minute | 525,600 | ~8 MB |
| IESO | HOEP | Hourly | 8,760 | ~200 KB |
| GB | System prices | 30-minute | 17,520 | ~400 KB |
| Germany | DA prices | 15-minute | 35,040 | ~600 KB |
| ENTSO-E | Generation by type (1 zone) | 15-min or hourly | 35,040 or 8,760 | ~1-3 MB |
| AESO | CSD generation (all units) | 5-minute | ~50M+ | ~500 MB-1 GB |

**Total estimated storage for core portfolio (5 years, 6-8 markets, price+demand+generation):** ~2-5 GB. Well within local disk. The only large dataset is AESO CSD unit-level generation — this can be stored separately and only downloaded for specific projects.

### Git Strategy

- **Small/medium data (prices, demand, aggregate generation):** Commit Parquet files to repo using Git LFS if >50 MB, or directly if smaller. This lets collaborators clone and immediately have data.
- **Large data (unit-level generation, BM-level data, weather):** `.gitignore` these directories. Provide download/backfill scripts that recreate them from APIs.
- **Collection scripts and metadata:** Always committed. The collection log lets anyone see what data exists and its freshness.

---

## Scheduled Data Collection

### Why Scheduling Matters

Without scheduling, you'll lose data as lookback windows expire. The scheduler runs daily (or weekly), pulls the latest data from each source, appends it to the Parquet store, and logs what it did.

### Scheduler Design

```python
# scheduler.py — Entry point for scheduled data collection

"""
Runs as a cron job, GitHub Action, or manual invocation.
Checks what data is already stored, identifies gaps, and fetches only new/missing data.

Usage:
  python -m elec_data.scheduler run              # Run all configured collections
  python -m elec_data.scheduler run --market aeso # Run only AESO collections
  python -m elec_data.scheduler backfill --market aeso --start 2020-01-01 --end 2024-12-31
  python -m elec_data.scheduler status            # Show collection status for all markets
"""
```

### Collection Jobs

| Job | Frequency | Source | What It Does |
|-----|-----------|--------|-------------|
| `aeso_prices` | Daily | gridstatus → AESO | Fetches yesterday's hourly pool prices, appends to `raw/aeso/prices/` |
| `aeso_demand` | Daily | gridstatus → AESO | Fetches yesterday's demand data |
| `aeso_generation` | Daily | gridstatus → AESO | Fetches yesterday's generation by fuel |
| `aeso_merit_order` | Weekly | AESO ETS (custom) | Checks for newly published merit order snapshots (2-month lag) |
| `aeso_csd` | Monthly | AESO CSD page (custom) | Downloads latest CSD generation data |
| `ieso_prices` | Daily | gridstatus → IESO | Fetches yesterday's HOEP |
| `ieso_demand` | Daily | gridstatus → IESO | Fetches yesterday's Ontario demand |
| `ieso_generation` | Daily | gridstatus → IESO | Fetches yesterday's generation by fuel |
| `ieso_global_adjustment` | Monthly | IESO website (custom) | Downloads latest GA data |
| `entsoe_prices` | Daily | entsoe-py | Fetches yesterday's DA prices for configured bidding zones |
| `entsoe_generation` | Daily | entsoe-py | Fetches yesterday's actual generation by type for configured zones |
| `entsoe_demand` | Daily | entsoe-py | Fetches yesterday's actual load for configured zones |
| `entsoe_flows` | Daily | entsoe-py | Fetches yesterday's cross-border physical flows |
| `bmrs_system_prices` | Daily | Elexon API (custom) | Fetches yesterday's SSP/SBP |
| `bmrs_generation` | Daily | Elexon API (custom) | Fetches yesterday's generation by fuel |
| `carbon_prices` | Weekly | Manual or custom | EU ETS / UK ETS settlement prices |

### Execution Options

The scheduler should support multiple execution environments, from simplest to most automated:

1. **Manual CLI** (`python -m elec_data.scheduler run`): Run from your terminal whenever you want. Good enough to start.
2. **Cron job** (local machine): Schedule via `crontab -e` to run daily at 6 AM. Zero cost. Requires your machine to be on.
3. **GitHub Actions** (scheduled workflow): Free for public repos (2,000 min/month). Runs on GitHub's servers, commits new data back to repo. Best option for this portfolio — it's visible, automated, and free.
4. **Cloud function** (AWS Lambda, GCP Cloud Function): Free tier available. Overkill unless you need sub-daily frequency.

**Recommended: GitHub Actions for daily runs.** The workflow would:
1. Check out the repo
2. Run the scheduler
3. If new data was collected, commit the updated Parquet files and push

### Backfill Process

For initial setup, each market needs a one-time historical backfill:

```python
# Backfill example — run once per market
python -m elec_data.scheduler backfill --market aeso --start 2020-01-01 --end 2025-12-31
python -m elec_data.scheduler backfill --market ieso --start 2020-01-01 --end 2025-12-31
python -m elec_data.scheduler backfill --market entsoe --zones DE_LU,FR,ES,GB,NL --start 2015-01-01 --end 2025-12-31
python -m elec_data.scheduler backfill --market bmrs --start 2020-01-01 --end 2025-12-31
```

**Backfill should be resumable.** If it fails halfway through (API rate limit, network error), re-running should pick up where it left off by checking the collection log.

### Rate Limiting & Error Handling

| Source | Known Rate Limits | Handling |
|--------|------------------|----------|
| gridstatus (AESO, IESO) | No documented limits, but be respectful. ISO websites can be slow. | 1-second delay between requests. Retry with exponential backoff (3 attempts). |
| ENTSO-E API | 400 requests per minute per API key. One request per day can only span up to 1 year. | Batch by year. Track request count. Sleep if approaching limit. |
| Elexon BMRS | No documented hard limit, but "fair use" expected. | 0.5-second delay between requests. Paginate large date ranges. |
| OMIE | No documented limits. | Standard respectful rate limiting. |

---

## Common Data Model

All data, regardless of source, is stored in one of these standardized schemas. This is the core original contribution of this toolkit.

### Prices Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime64[ns, UTC] | Start of the delivery period, in UTC |
| `market` | string | Market identifier (e.g., "AESO", "IESO", "DE_LU", "GB") |
| `price` | float64 | Energy price in local currency per MWh |
| `currency` | string | ISO 4217 code (CAD, USD, EUR, GBP) |
| `price_eur_mwh` | float64 | Nullable. Price converted to EUR/MWh for cross-market comparison (using daily exchange rate) |
| `price_type` | string | "day_ahead", "real_time", "balancing_buy", "balancing_sell", "pool" |
| `resolution_minutes` | int | Native resolution of this row (e.g., 60, 30, 15, 5, 1) |
| `source` | string | Data source identifier (e.g., "gridstatus_aeso", "entsoe", "bmrs") |

### Demand Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime64[ns, UTC] | Period start |
| `market` | string | Market identifier |
| `demand_mw` | float64 | System demand in MW |
| `demand_type` | string | "actual", "forecast_day_ahead", "forecast_intraday" |
| `resolution_minutes` | int | Native resolution |
| `source` | string | Data source |

### Generation Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime64[ns, UTC] | Period start |
| `market` | string | Market identifier |
| `fuel_type` | string | Harmonized fuel type (see fuel type mapping below) |
| `generation_mw` | float64 | Average generation in MW over the period |
| `resolution_minutes` | int | Native resolution |
| `source` | string | Data source |

### Cross-Border Flow Schema

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_utc` | datetime64[ns, UTC] | Period start |
| `from_market` | string | Exporting market |
| `to_market` | string | Importing market |
| `flow_mw` | float64 | Physical flow in MW (positive = in direction of from→to) |
| `resolution_minutes` | int | Native resolution |
| `source` | string | Data source |

### Harmonized Fuel Type Mapping

Each ISO uses different names for fuel types. The toolkit maps them all to a standard taxonomy:

| Standard Fuel Type | AESO | IESO | ENTSO-E | Elexon (GB) |
|-------------------|------|------|---------|-------------|
| `coal` | COAL | — (Ontario has no coal) | Fossil Hard coal, Fossil Brown coal/Lignite | COAL |
| `gas` | GAS | GAS | Fossil Gas | CCGT, OCGT |
| `nuclear` | — (Alberta has no nuclear) | NUCLEAR | Nuclear | NUCLEAR |
| `hydro` | HYDRO | HYDRO | Hydro Run-of-river, Hydro Water Reservoir, Hydro Pumped Storage | PS, NPSHYD |
| `wind` | WIND | WIND | Wind Onshore, Wind Offshore | WIND |
| `solar` | SOLAR | SOLAR | Solar | SOLAR |
| `biomass` | BIOMASS | BIOMASS | Biomass | BIOMASS |
| `other_renewable` | OTHER | BIOFUEL | Geothermal, Marine, Other renewable | OTHER |
| `other` | DUAL_FUEL, ENERGY_STORAGE | — | Fossil Oil, Other, Waste | OIL, OTHER |
| `storage` | ENERGY_STORAGE | — | Hydro Pumped Storage (consuming) | PS (consuming) |

**Note:** This mapping will need refinement through testing. The ENTSO-E "Other" category is notorious for containing misclassified CCGT generation (see Hirth et al. 2018). The toolkit should document known data quality issues per market.

---

## Query Interface

The user-facing API should be simple and Pythonic:

```python
from elec_data import Toolkit

# Initialize — points to your data directory
tk = Toolkit(data_dir="./data")

# Get prices for multiple markets, harmonized to hourly resolution
prices = tk.get_prices(
    markets=["AESO", "IESO", "GB", "DE_LU", "ES"],
    start="2023-01-01",
    end="2024-01-01",
    resolution="hourly",     # Resamples to common resolution
    currency="EUR",           # Optional: convert all to EUR
    pivot=True                # Returns wide-format: columns = markets
)

# Get generation mix for a single market
gen = tk.get_generation(
    markets=["AESO"],
    start="2024-06-01",
    end="2024-06-30",
    fuel_types=["gas", "wind", "solar", "coal"]  # Filter specific fuels
)

# Get cross-border flows
flows = tk.get_flows(
    from_market="FR",
    to_market="ES",
    start="2023-01-01",
    end="2024-01-01"
)

# Check data availability
tk.status()
# Returns a DataFrame showing: market, data_type, earliest_date, latest_date, total_rows, gaps
```

### Smart Fetching

The query interface should check local storage first and only hit APIs for missing data:

```
User requests AESO prices 2023-01-01 to 2024-12-31
  → Check collection_log: local data exists 2023-01-01 to 2025-01-15
  → All requested data is local → read from Parquet, return

User requests AESO prices 2024-06-01 to 2025-06-01
  → Check collection_log: local data exists through 2025-02-17
  → Fetch 2025-02-18 to 2025-06-01 from API
  → Append to store, update log
  → Combine local + new data, return
```

---

## Market Metadata Registry

A JSON file containing structured reference data about each market. This is used by the harmonizer, the query interface, and also serves as documentation for downstream projects.

```json
{
  "AESO": {
    "full_name": "Alberta Electric System Operator",
    "country": "CA",
    "region": "Alberta",
    "timezone": "America/Edmonton",
    "currency": "CAD",
    "market_type": "energy_only",
    "price_node_structure": "single_zone",
    "settlement_periods_per_hour": 1,
    "price_cap_mwh": 999.99,
    "price_floor_mwh": 0,
    "native_price_resolution_minutes": 60,
    "native_demand_resolution_minutes": 1,
    "native_generation_resolution_minutes": 60,
    "data_source_primary": "gridstatus",
    "data_source_supplementary": ["aeso_ets", "aeso_csd"],
    "api_key_required": false,
    "notes": "Transitioning to nodal pricing (REM) with LMP and capacity market. Current single-zone pool price. SMP reported at 1-minute resolution. Pool price is the hourly time-weighted average of SMP.",
    "interconnections": ["BC", "Montana", "Saskatchewan"]
  },
  "IESO": {
    "full_name": "Independent Electricity System Operator",
    "country": "CA",
    "region": "Ontario",
    "timezone": "America/Toronto",
    "currency": "CAD",
    "market_type": "hybrid",
    "price_node_structure": "single_zone",
    "settlement_periods_per_hour": 1,
    "price_cap_mwh": 2000,
    "price_floor_mwh": -2000,
    "native_price_resolution_minutes": 60,
    "native_demand_resolution_minutes": 60,
    "native_generation_resolution_minutes": 60,
    "data_source_primary": "gridstatus",
    "data_source_supplementary": ["ieso_xml_reports"],
    "api_key_required": false,
    "notes": "Hybrid market: HOEP sets wholesale spot price but Global Adjustment (out-of-market contracts) represents ~80%+ of total cost. Total cost = HOEP + GA. Negative prices possible.",
    "interconnections": ["Quebec", "Manitoba", "New York", "Michigan", "Minnesota"]
  },
  "DE_LU": {
    "full_name": "Germany-Luxembourg Bidding Zone",
    "country": "DE",
    "region": "Germany + Luxembourg",
    "timezone": "Europe/Berlin",
    "currency": "EUR",
    "market_type": "energy_only_with_capacity_reserve",
    "price_node_structure": "single_zone",
    "settlement_periods_per_hour": 4,
    "price_cap_mwh": 4000,
    "price_floor_mwh": -500,
    "native_price_resolution_minutes": 15,
    "native_demand_resolution_minutes": 15,
    "native_generation_resolution_minutes": 15,
    "data_source_primary": "entsoe",
    "api_key_required": true,
    "api_key_env_var": "ENTSOE_API_KEY",
    "notes": "Largest European power market. Negative prices frequent with high wind/solar penetration. Part of CWE market coupling. 15-minute settlement since 2011.",
    "interconnections": ["FR", "NL", "BE", "AT", "CH", "CZ", "PL", "DK1", "DK2", "SE4", "NO2"]
  },
  "GB": {
    "full_name": "Great Britain",
    "country": "GB",
    "region": "England, Wales, Scotland",
    "timezone": "Europe/London",
    "currency": "GBP",
    "market_type": "bilateral_with_balancing_mechanism",
    "price_node_structure": "single_zone",
    "settlement_periods_per_hour": 2,
    "price_cap_mwh": 6000,
    "price_floor_mwh": -6000,
    "native_price_resolution_minutes": 30,
    "native_demand_resolution_minutes": 30,
    "native_generation_resolution_minutes": 30,
    "data_source_primary": "bmrs",
    "data_source_supplementary": ["entsoe"],
    "api_key_required": true,
    "api_key_env_var": "BMRS_API_KEY",
    "notes": "Bilateral market — no central day-ahead mandatory pool. System prices (SSP/SBP) set through Balancing Mechanism. Day-ahead 'prices' from N2EX/EPEX coupling. 30-minute settlement periods. Capacity Market with T-4 and T-1 auctions.",
    "interconnections": ["FR", "NL", "BE", "NO2", "DK1", "IE"]
  },
  "ES": {
    "full_name": "Spain",
    "country": "ES",
    "region": "Iberian Peninsula (Spain)",
    "timezone": "Europe/Madrid",
    "currency": "EUR",
    "market_type": "pool",
    "price_node_structure": "single_zone",
    "settlement_periods_per_hour": 1,
    "price_cap_mwh": 3000,
    "price_floor_mwh": 0,
    "native_price_resolution_minutes": 60,
    "native_demand_resolution_minutes": 60,
    "native_generation_resolution_minutes": 60,
    "data_source_primary": "entsoe",
    "data_source_supplementary": ["omie"],
    "api_key_required": true,
    "api_key_env_var": "ENTSOE_API_KEY",
    "notes": "Part of MIBEL (Mercado Ibérico de Electricidad) with Portugal. Subject to Iberian Exception gas price cap mechanism (2022-2024). Hourly settlement. High solar penetration driving increasing negative/near-zero price hours.",
    "interconnections": ["FR", "PT"]
  }
}
```

---

## Project Structure

```
open-electricity-data-toolkit/
├── README.md                          # This file
├── pyproject.toml                     # Package metadata, dependencies
├── LICENSE                            # MIT
│
├── src/
│   └── elec_data/
│       ├── __init__.py
│       ├── toolkit.py                 # Main Toolkit class (user-facing query interface)
│       │
│       ├── collectors/                # Data collection layer
│       │   ├── __init__.py
│       │   ├── base.py                # Abstract base collector
│       │   ├── gridstatus_collector.py    # Wraps gridstatus for AESO, IESO, US ISOs
│       │   ├── entsoe_collector.py        # Wraps entsoe-py for European markets
│       │   ├── bmrs_collector.py          # Custom Elexon BMRS client
│       │   ├── aeso_supplemental.py       # Merit order, CSD data (not in gridstatus)
│       │   ├── ieso_supplemental.py       # GA data, XML reports (not in gridstatus)
│       │   └── carbon_collector.py        # EU ETS / UK ETS prices
│       │
│       ├── harmonize/                 # Data harmonization layer
│       │   ├── __init__.py
│       │   ├── schemas.py             # Dataclass/Pydantic models for common schemas
│       │   ├── fuel_mapping.py        # Fuel type normalization per market
│       │   ├── resampler.py           # Granularity alignment (upsample/downsample)
│       │   ├── timezone.py            # UTC conversion and local time handling
│       │   └── currency.py            # Currency conversion (optional, daily rates)
│       │
│       ├── storage/                   # Storage management
│       │   ├── __init__.py
│       │   ├── parquet_store.py       # Read/write/append Parquet files
│       │   ├── collection_log.py      # Track what's been collected and when
│       │   ├── quality_checks.py      # Anomaly detection, gap identification
│       │   └── catalog.py             # DuckDB catalog management
│       │
│       ├── scheduler/                 # Scheduled data collection
│       │   ├── __init__.py
│       │   ├── runner.py              # Main scheduler runner
│       │   ├── jobs.py                # Job definitions (what to collect, when)
│       │   └── backfill.py            # Historical backfill logic
│       │
│       └── registry/                  # Market metadata
│           ├── __init__.py
│           ├── markets.py             # Market metadata access
│           └── market_registry.json   # The registry file
│
├── data/                              # Local data store (partially gitignored)
│   ├── raw/                           # See Storage Layout above
│   └── metadata/
│
├── tests/
│   ├── test_collectors/
│   ├── test_harmonize/
│   ├── test_storage/
│   └── test_integration/              # End-to-end tests with sample data
│
├── examples/
│   ├── 01_quickstart.ipynb            # Basic setup and first queries
│   ├── 02_compare_prices.ipynb        # Cross-market price comparison
│   ├── 03_generation_mix.ipynb        # Generation by fuel across markets
│   ├── 04_backfill_demo.ipynb         # How to backfill historical data
│   └── 05_custom_analysis.ipynb       # Using DuckDB for custom SQL queries
│
├── .github/
│   └── workflows/
│       ├── test.yml                   # Run tests on every PR
│       └── collect_data.yml           # Scheduled daily data collection
│
├── .gitignore                         # Ignore large data files
└── .env.example                       # Template for API keys
```

---

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1-2)

**Goal:** Get the skeleton working end-to-end for one market (AESO).

1. Set up Python package structure with `pyproject.toml`
2. Implement `parquet_store.py` — basic read/write/append Parquet files
3. Implement `collection_log.py` — track what's been collected
4. Implement `gridstatus_collector.py` — wrapper for AESO price, demand, generation
5. Implement price schema in `schemas.py`
6. Implement `toolkit.py` — basic `get_prices()` for a single market
7. Write basic tests
8. Create `01_quickstart.ipynb` example
9. Backfill AESO data: 2020-present

**Milestone:** `tk.get_prices(markets=["AESO"], start="2023-01-01", end="2024-01-01")` works.

### Phase 2: Multi-Market (Week 2-4)

**Goal:** Add IESO and European markets, implement harmonization.

1. Add IESO to `gridstatus_collector.py`
2. Implement `entsoe_collector.py` — prices, generation, demand for DE, FR, ES, GB
3. Implement `bmrs_collector.py` — GB system prices and generation
4. Implement `resampler.py` — granularity alignment
5. Implement `timezone.py` — UTC conversion
6. Implement `fuel_mapping.py` — harmonized fuel types
7. Extend `toolkit.py` for multi-market queries
8. Backfill all configured markets
9. Create `02_compare_prices.ipynb` and `03_generation_mix.ipynb`

**Milestone:** `tk.get_prices(markets=["AESO", "IESO", "GB", "DE_LU", "ES"], resolution="hourly")` works with harmonized output.

### Phase 3: Scheduling & Supplemental Data (Week 4-6)

**Goal:** Automated daily collection, supplemental data sources.

1. Implement `scheduler/runner.py` and `scheduler/jobs.py`
2. Create GitHub Actions workflow for daily collection
3. Implement `aeso_supplemental.py` — merit order snapshots, CSD generation
4. Implement `ieso_supplemental.py` — Global Adjustment data
5. Implement `quality_checks.py` — gap detection, anomaly flagging
6. Implement `catalog.py` — DuckDB views over Parquet store
7. Implement `backfill.py` — resumable historical backfill
8. Add cross-border flow collection
9. Create `04_backfill_demo.ipynb` and `05_custom_analysis.ipynb`

**Milestone:** GitHub Actions runs daily, data grows automatically, `tk.status()` shows all markets green.

### Phase 4: Polish & Documentation (Week 6-8)

**Goal:** Production-ready quality.

1. Add currency conversion (`currency.py`)
2. Add `market_registry.json` with full metadata for all markets
3. Comprehensive test suite with mocked API responses
4. Documentation: docstrings, README examples, market-specific notes
5. CI/CD: linting (ruff), formatting (black), type checking (mypy), test coverage
6. Add carbon price collection
7. Create a `status` dashboard notebook showing data coverage heatmap

**Milestone:** Package is installable, well-documented, all tests pass, data collection is running in CI.

---

## Technical Requirements

### Dependencies

```toml
[project]
name = "elec-data"
requires-python = ">=3.11"
dependencies = [
    # Data collection
    "gridstatus>=0.29",          # North American ISO data
    "entsoe-py>=0.6",            # ENTSO-E Transparency Platform
    "requests>=2.31",            # HTTP client for custom API clients
    
    # Data processing
    "pandas>=2.2",
    "pyarrow>=15.0",             # Parquet read/write
    "duckdb>=0.10",              # Analytical query engine
    
    # Utilities
    "python-dotenv>=1.0",        # API key management
    "tqdm>=4.64",                # Progress bars for backfills
    "pydantic>=2.5",             # Data validation and schemas
    "pytz",                      # Timezone handling
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov",
    "ruff",
    "black",
    "mypy",
    "pandas-stubs",
]
viz = [
    "plotly>=5.0",
    "jupyter",
]
```

### API Keys Required

| Source | Key Required? | How to Get |
|--------|--------------|-----------|
| gridstatus (open-source) | No | N/A |
| ENTSO-E Transparency Platform | Yes (free) | Register at https://transparency.entsoe.eu → email with token |
| Elexon BMRS | Yes (free) | Register at https://bmrs.elexon.co.uk → API key in profile |
| OMIE | No | Public data downloads |
| ERA5 (weather) | Yes (free) | Register at https://cds.climate.copernicus.eu |

### Environment Setup

```bash
# .env file (never committed)
ENTSOE_API_KEY=your_token_here
BMRS_API_KEY=your_key_here
CDS_API_KEY=your_key_here
```

---

## Design Decisions & Rationale

| Decision | Rationale |
|----------|-----------|
| **Use `gridstatus` as dependency, don't rewrite** | It's open-source (BSD-3), well-maintained, covers AESO + IESO + US ISOs. Writing our own scrapers adds no portfolio value and creates maintenance burden. |
| **Store at native resolution, resample on query** | Downsampling destroys information. If you store AESO at hourly but later need 1-minute SMP data for a volatility analysis, you're stuck. Store native, resample on read. |
| **Parquet over CSV** | Columnar compression (10-30x smaller than CSV), type preservation (timestamps stay as timestamps), partition-friendly, native DuckDB support. |
| **DuckDB over SQLite** | DuckDB is purpose-built for analytical queries on Parquet files. It can query Parquet directly without loading into a database. SQLite is row-oriented and can't do this. |
| **UTC internal, local on display** | Eliminates all DST ambiguity. Cross-market alignment is trivial in UTC. Local times are only useful for display ("morning peak is 7-9 AM local"). |
| **GitHub Actions for scheduling over cloud services** | Free for public repos, visible in the repo, doesn't require cloud account setup. For a portfolio project, transparency of the data pipeline is itself a signal. |
| **Collection log as data** | Treating the collection log as a queryable Parquet file means you can answer "when was my data last updated?" and "are there any gaps?" with the same tools you use for the data itself. |
| **Fuel type harmonization as explicit mapping table** | Different projects may need different groupings (e.g., "thermal" = coal + gas + oil, or "clean" = nuclear + hydro + wind + solar). The explicit mapping table is easy to modify or extend. |
| **Market registry as JSON** | Human-readable, easy to extend, serves as documentation. Could be loaded into Pydantic models for validation. |

---

## Relationship to Portfolio Projects

This toolkit is the foundation for all other repos in the portfolio:

| Portfolio Project | Toolkit Data Used |
|-------------------|-------------------|
| Alberta Pool Price Forecasting | `tk.get_prices(["AESO"])`, `aeso_supplemental` merit order data, `aeso_supplemental` CSD generation |
| Ontario Market Analysis | `tk.get_prices(["IESO"])`, `ieso_supplemental` GA data, `tk.get_generation(["IESO"])`, `tk.get_flows()` for inter-ties |
| European Day-Ahead Comparison | `tk.get_prices(["DE_LU","FR","ES","GB","NL","NO1",...])`, `tk.get_generation()`, `tk.get_flows()` |
| GB Balancing Mechanism | `bmrs_collector` system prices, BOA data, generation |
| Spain Renewables Integration | `tk.get_prices(["ES"])`, `tk.get_generation(["ES"])`, `tk.get_flows(from="ES", to="FR")` |
| Battery Storage Valuation | `tk.get_prices()` for all markets at highest available resolution |
| Transmission Congestion | `tk.get_flows()`, `tk.get_prices()` for cross-border differentials |
| Renewable Forecasting | `tk.get_generation()` filtered to wind/solar, plus weather data |
| Demand Climate Sensitivity | `tk.get_demand()` for all markets, plus weather data |
| Carbon Pricing Impact | `tk.get_prices()` + `carbon_collector` EU/UK ETS data |
| Energy Transition Modelling | `tk.get_generation()` for calibration, `tk.get_demand()` for scenarios |
