# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Critical Rules

1. **Never guess. Use official documentation.** All decisions must be verified against the actual Census Bureau API (https://api.census.gov/data.json) or the Singer SDK docs (https://sdk.meltano.com). Use Context7 or hit the API directly. It is better to say "I don't know" than to produce something wrong.
2. **Never make up endpoints, variables, or API behavior.** The Census API has quirks that differ between vintages. Always verify with a live `curl` call before coding against an endpoint.
3. **Follow reference tap patterns.** The sibling repositories `../tap-fred`, `../tap-fmp`, `../tap-massive`, `../tap-yfinance` are the canonical reference for Meltano Singer SDK tap structure.

## What This Is

A Meltano Singer tap for the U.S. Census Bureau API. Extracts population estimates, decennial census counts, and county geography (lat/lon centroids) for use in population-weighted energy demand forecasting (HDD/CDD). Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Build & Test Commands

```bash
uv sync                                    # Install dependencies
uv run pytest                              # Run all tests
uv run pytest tests/test_core.py -k name   # Run tests matching pattern
uv run tap-census --about                  # Show tap info and settings
uv run tap-census --discover               # Validate Singer discovery
```

## Architecture

### File Structure

```
tap_census/
├── __init__.py
├── __main__.py          # TapCensus.cli()
├── tap.py               # TapCensus class, config schema, stream registration
├── client.py            # CensusStream base class (RESTStream), retry, rate limiting
├── helpers.py            # parse_census_array, make_fips, safe_int, safe_float
└── streams/
    ├── __init__.py
    ├── population_streams.py   # PEP + Decennial population streams
    └── geography_streams.py    # Gazetteer county centroid stream
```

### Base Class Hierarchy

```
CensusStream (ABC, RESTStream)       # Rate limiting, backoff retry, 2D array parsing
├── PepPopulationBaseStream          # Shared PEP logic (vintage routing, date_code vs year_suffix)
│   ├── CountyPopulationStream       # County PEP (2010-2019 via 2019 vintage)
│   └── StatePopulationStream        # State PEP (2010-2021 via 2019+2021 vintages)
└── DecennialPopulationStream        # Decennial county pop (2000, 2010, 2020)

Stream (base SDK class)
└── CountyGeographyStream            # Gazetteer zip download, not REST
```

### Error Handling

- `strict_mode` config (default: `false`): When true, API errors raise exceptions. When false, errors are logged as warnings and the partition is skipped.
- `_fetch_census_data()` in the base class consolidates the request + error handling pattern.

## Census Bureau API — Verified Facts

**Everything below was verified via live `curl` calls, NOT from PROMPT.md or assumed.**

### PEP Population Estimates (`pep/population`)

The PEP API changed its variable naming scheme between vintages:

| Vintage | Date Variable | Name Variable | County Support | Years Covered |
|---------|--------------|---------------|----------------|---------------|
| 2015 | `DATE_` | `GEONAME` | Yes | 2010-2015 |
| 2016-2017 | `DATE_` | `GEONAME` | Yes | 2010-vintage year |
| 2018 | `DATE_CODE` | `GEONAME` | Yes | 2010-2018 |
| 2019 | `DATE_CODE` | `NAME` | Yes | 2010-2019 |
| 2021 | `POP_{year}`, `DENSITY_{year}` | `NAME` | **NO** (state only) | 2020-2021 |
| 2022-2023 | N/A | N/A | N/A | **DO NOT EXIST** |

**DATE_CODE mapping (2019 vintage):** `DATE_CODE = year - 2007` (e.g., 2019 → 12, 2015 → 8, 2010 → 3).

**Current implementation uses the 2019 vintage** for all county/state data (2010-2019) because it has the most consistent variable names (`DATE_CODE`, `NAME`) and broadest geography support.

### PEP Characteristics (`pep/charv`) — 2023 Vintage

A newer endpoint that provides county-level population for 2020-2023:
- Variables: `POP`, `YEAR`, `MONTH`, `AGE`, `SEX`, `HISP`, `NAME`
- Filter for total population: `AGE=0000&SEX=0&HISP=0`
- County support: **Yes** (3,222 counties confirmed)
- No `DENSITY` variable available
- **Not yet implemented** — should be added to cover 2020-2023 county data gap.

### Decennial Census

| Year | Endpoint | Pop Variable | Verified |
|------|----------|-------------|----------|
| 2000 | `/2000/dec/sf1` | `P001001` | Yes |
| 2010 | `/2010/dec/sf1` | `P001001` | Yes |
| 2020 | `/2020/dec/pl` | `P1_001N` | Yes |

### PEP Housing (`pep/housing`) — 2013-2019 Vintages

- Variables: `HUEST` (housing unit estimate), `DATE_CODE`
- County support: Yes
- **Not yet implemented.**

### County Geography (Gazetteer)

- Source: `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2020_Gazetteer/2020_Gaz_counties_national.zip`
- Format: Tab-delimited text inside a zip archive
- Columns: `USPS, GEOID, ANSICODE, NAME, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPTLAT, INTPTLONG`
- The 2023 Gazetteer URL pattern (`2023_Gazetteer/...`) returns **404**. Only 2020 is confirmed available.

### Census API Quirks

- Returns **2D arrays** (first row = headers, subsequent rows = data), NOT JSON objects.
- **All values are strings**, even numbers. Always cast.
- Returns `"null"` as the literal string `"null"`, not JSON null.
- FIPS codes are **zero-padded strings** ("06" not 6, "037" not 37).
- Variable names and geography support **change between vintages** — always verify.
- API key is passed as a query parameter (`&key=YOUR_KEY`), not a header.

## Meltano / Singer SDK Patterns

### Keeping Config in Sync

When adding/changing settings, update all three in the same commit:
1. `config_jsonschema` in `tap_census/tap.py`
2. `settings` block in `meltano.yml`
3. Environment variables in `.env.example`

### Setting Kind Mappings

| Python Type | Meltano Kind |
|-------------|--------------|
| `StringType` | `string` |
| `IntegerType` | `integer` |
| `BooleanType` | `boolean` |
| `NumberType` | `number` |
| `DateTimeType` | `date_iso8601` |
| `ArrayType` | `array` |

Properties with `secret=True` → `sensitive: true` in meltano.yml.

## Code Style

- **DRY / SOLID**: Extract shared logic into base classes (e.g., `PepPopulationBaseStream`). Consolidate duplicate patterns (e.g., `_safe_cast` for int/float conversion, `_fetch_census_data` for request + error handling).
- **Self-documenting names**: `requires_county_geography`, `transform_census_record_to_output`, `get_geography_params`.
- **Minimal loops**: Only use `for`/`while` when necessary. Prefer list comprehensions and generators.
- **Use UV** for all Python execution (`uv sync`, `uv run pytest`, etc.).

## Not Yet Implemented

These are confirmed available in the Census API but not yet built:

- `pep/charv` (2023 vintage): County population 2020-2023 — fills the gap where `pep/population` 2021 vintage lacks county geography.
- `pep/housing` (2013-2019 vintages): Housing unit estimates at county level.
- `pep/components` (2015-2019 vintages): Components of change (births, deaths, migration).
- ACS endpoints: Detailed demographics, housing characteristics. Lower priority.
