# Prompt: Build `tap-census` — A Singer Tap for U.S. Census Bureau Population Data

## Goal
Build a simple, production-grade Singer tap (Meltano SDK) that extracts population data from the U.S. Census Bureau API. This data is needed for computing population-weighted Heating Degree Days (HDD) and Cooling Degree Days (CDD) from weather forecasts — a critical feature for energy demand forecasting.

**This is the simplest tap of the set.** The Census API is straightforward, the data is small (county populations updated annually), and there's no complex pagination or rate limiting. Build it in half a day.

This is an extractor ONLY — no loader logic.

---

## Reference Codebases (MUST study these first)

1. **tap-fred** at `~/code/github/personal/tap-fred/` — Config, caching, error handling patterns
2. **tap-fmp** at `~/code/github/personal/tap-fmp/` — Simple stream patterns

---

## Census Bureau API Documentation

### Base URL
```
https://api.census.gov/data/
```

### Authentication
- API key recommended (free): https://api.census.gov/data/key_signup.html
- Without key: 500 requests/day limit
- With key: no documented limit (but be respectful)
- Passed as query parameter: `&key=YOUR_KEY`

### Datasets We Need

**1. Population Estimates Program (PEP)** — Annual county/state population
```
https://api.census.gov/data/{year}/pep/population
```
- Available years: 2015-2023 (varies by vintage)
- Provides: total population, components of change
- Geographic levels: nation, state, county, metro area

**2. Decennial Census** — Every 10 years (most accurate)
```
https://api.census.gov/data/{year}/dec/pl
```
- Years: 2000, 2010, 2020
- Most accurate population counts

**3. American Community Survey (ACS)** — Detailed demographics (optional, lower priority)
```
https://api.census.gov/data/{year}/acs/acs5
```
- 5-year estimates, annual release
- Has housing units, households (relevant for energy demand modeling)

### API Query Pattern
```
GET https://api.census.gov/data/2023/pep/population?get=POP_2023,NAME&for=county:*&in=state:*&key=YOUR_KEY
```

Parameters:
- `get` — Variables to retrieve (comma-separated)
- `for` — Geography level and filter (e.g., `county:*` for all counties)
- `in` — Parent geography (e.g., `state:*` for all states)
- `key` — API key

### Response Format
The Census API returns a **2D array** (NOT JSON objects):
```json
[
  ["POP_2023", "NAME", "state", "county"],
  ["4940672", "Maricopa County, Arizona", "04", "013"],
  ["1043433", "Pima County, Arizona", "04", "019"],
  ...
]
```
- First row is headers
- Subsequent rows are data
- All values are strings (must cast numbers)

### Key Variables

**PEP Population:**
| Variable | Description |
|----------|-------------|
| `POP_20XX` | Total population for year 20XX |
| `DENSITY_20XX` | Population density |
| `NAME` | Geographic area name |
| `STATE` | State FIPS code |
| `COUNTY` | County FIPS code |

**Decennial:**
| Variable | Description |
|----------|-------------|
| `P1_001N` | Total population (2020) |
| `NAME` | Geographic area name |
| `STATE` | State FIPS code |
| `COUNTY` | County FIPS code |

### Geographic Identifiers (FIPS Codes)
- State: 2-digit FIPS (e.g., "06" = California)
- County: 3-digit FIPS within state (e.g., "037" = Los Angeles County)
- Full county FIPS: state + county = "06037"
- These FIPS codes are what you'll join to weather grid points (lat/lon → county → population weight)

### Available Geography Levels
- `us:*` — National total
- `state:*` — All states
- `county:*` — All counties (in `state:*`)
- `metropolitan statistical area/micropolitan statistical area:*` — Metro areas
- `place:*` — Cities/towns

---

## Directory Structure

```
tap-census/
├── tap_census/
│   ├── __init__.py
│   ├── __main__.py          # TapCensus.cli()
│   ├── tap.py               # Main Tap class, config, stream registration
│   ├── client.py            # CensusStream base class, request handling
│   ├── helpers.py           # Array-to-dict conversion, FIPS utilities
│   └── streams/
│       ├── __init__.py
│       ├── population_streams.py  # PEP and Decennial population
│       └── geography_streams.py   # FIPS code reference data
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```

---

## Implementation Requirements

### 1. Tap Class (`tap.py`)

```python
class TapCensus(Tap):
    name = "tap-census"
```

**Config Properties:**
- `api_key` (StringType, optional, secret) — Census API key. If omitted, 500 req/day limit.
- `api_url` (StringType, default "https://api.census.gov/data")
- `years` (ArrayType(IntegerType), default [2020, 2021, 2022, 2023]) — Which years to pull population for
- `geography_levels` (ArrayType(StringType), default ["county"]) — `["county", "state"]`, etc.
- `datasets` (ArrayType(StringType), default ["pep"]) — `["pep"]`, `["decennial"]`, `["pep", "decennial"]`
- `states` (ArrayType(StringType), default ["*"]) — Filter by state FIPS. `["*"]` = all. `["06", "36"]` = CA + NY.
- `max_requests_per_minute` (IntegerType, default 60)
- `strict_mode` (BooleanType, default False)

### 2. Base Stream Class (`client.py`)

```python
class CensusStream(RESTStream, ABC):
    url_base = "https://api.census.gov/data"
```

**Key differences from other taps:**
- Census returns 2D arrays, NOT JSON objects. You must convert:
  ```python
  def parse_response(self, response) -> Iterable[dict]:
      data = response.json()
      if not data or len(data) < 2:
          return
      headers = [h.lower() for h in data[0]]
      for row in data[1:]:
          record = dict(zip(headers, row))
          yield record
  ```
- This is the main non-standard thing about this tap.

**Request with Retry:**
```python
@backoff.on_exception(backoff.expo, (RequestException,), max_tries=5, max_time=60)
def _make_request(self, url, params):
    self._throttle()
    response = self.requests_session.get(url, params=params, timeout=(10, 30))
    response.raise_for_status()
    return response.json()
```

### 3. Streams

**Stream 1: `CountyPopulationStream`** — County-level population by year
- **Partitioned by year**
- Path: `/{year}/pep/population`
- Query: `get=POP_{year},DENSITY_{year},NAME&for=county:*&in=state:*`
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("year", th.IntegerType, required=True),
      th.Property("state_fips", th.StringType, required=True),
      th.Property("county_fips", th.StringType, required=True),
      th.Property("fips", th.StringType, required=True),           # state + county concatenated
      th.Property("name", th.StringType),
      th.Property("population", th.IntegerType),
      th.Property("density", th.NumberType),
  ).to_dict()
  ```
- Primary keys: `["year", "fips"]`
- `post_process()`:
  - Cast population/density to numbers
  - Concatenate state + county FIPS into full 5-digit code
  - Add `year` from partition context

**Stream 2: `StatePopulationStream`** — State-level population
- Same pattern but `for=state:*`
- Schema: year, state_fips, name, population, density
- Primary keys: `["year", "state_fips"]`

**Stream 3: `DecennialPopulationStream`** — Decennial census (2000, 2010, 2020)
- Path: `/{year}/dec/pl`
- Query: `get=P1_001N,NAME&for=county:*&in=state:*`
- Only for years [2000, 2010, 2020]
- Schema: year, state_fips, county_fips, fips, name, population
- Primary keys: `["year", "fips"]`

**Stream 4: `CountyGeographyStream`** — FIPS code reference table
- This is a utility stream that maps county FIPS to lat/lon centroids
- Source: Census Gazetteer files (downloadable CSVs)
  - URL: `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.txt`
  - Tab-delimited file with columns: USPS, GEOID, ANSICODE, NAME, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPTLAT, INTPTLONG
- Download once, parse, emit records
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("fips", th.StringType, required=True),         # GEOID
      th.Property("state_abbrev", th.StringType),                # USPS
      th.Property("name", th.StringType),
      th.Property("latitude", th.NumberType, required=True),     # INTPTLAT (centroid)
      th.Property("longitude", th.NumberType, required=True),    # INTPTLONG (centroid)
      th.Property("land_area_sqmi", th.NumberType),
      th.Property("water_area_sqmi", th.NumberType),
  ).to_dict()
  ```
- Primary keys: `["fips"]`
- **This stream is critical** — it's how you map weather grid points to counties for population weighting

### 4. Helpers (`helpers.py`)

```python
def parse_census_array(data: list[list[str]]) -> list[dict]:
    """Convert Census API 2D array response to list of dicts."""
    if not data or len(data) < 2:
        return []
    headers = [h.lower().replace(" ", "_") for h in data[0]]
    return [dict(zip(headers, row)) for row in data[1:]]

def make_fips(state_fips: str, county_fips: str) -> str:
    """Create full 5-digit FIPS code from state + county."""
    return f"{state_fips.zfill(2)}{county_fips.zfill(3)}"

def safe_int(value: str | None) -> int | None:
    """Safely convert Census string value to integer."""
    if value is None or value == "" or value == "null":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_float(value: str | None) -> float | None:
    """Safely convert Census string value to float."""
    if value is None or value == "" or value == "null":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
```

### 5. Key Considerations

**This is simple. Don't over-engineer it.**
- Total data: ~3,200 counties × ~10 years = ~32,000 records for county population
- Plus ~50 states × ~10 years = ~500 records for state population
- Plus ~3,200 county geography records
- Total: ~35,000 records. Tiny.
- No need for complex pagination (Census returns all counties in one response)
- No need for complex rate limiting (well under limits)

**PEP API Year Availability:**
- PEP vintage changes yearly. The 2023 PEP vintage has `POP_2020` through `POP_2023`.
- For older years, you may need to query older vintages:
  - `2019/pep/population` for 2010-2019
  - `2021/pep/population` for 2020-2021
- Handle this by trying each year's endpoint and falling back gracefully

**Variable Names Change Between Years:**
- 2020 PEP: `POP_2020`
- 2023 PEP: `POP_2023`
- The variable name includes the year — construct it dynamically: `f"POP_{year}"`

**Census API Quirks:**
- Returns ALL values as strings (even numbers). Always cast.
- Returns `null` as the string `"null"`, not JSON null. Handle this.
- County FIPS within state: 3 digits, zero-padded. State: 2 digits, zero-padded.
- Some API endpoints may 404 for certain year/dataset combinations. Log WARNING, skip.

### 6. pyproject.toml

```toml
[project]
name = "tap-census"
version = "0.0.1"
description = "Singer tap for U.S. Census Bureau population and geographic data"
requires-python = ">=3.10,<4.0"

dependencies = [
    "singer-sdk~=0.53.5",
    "requests~=2.32.3",
    "backoff>=2.2.1,<3.0.0",
]

[project.scripts]
tap-census = "tap_census.tap:TapCensus.cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### 7. meltano.yml

```yaml
version: 1
send_anonymous_usage_stats: false
project_id: "tap-census"

plugins:
  extractors:
  - name: tap-census
    namespace: tap_census
    pip_url: -e .
    capabilities:
      - state
      - catalog
      - discover
      - about
      - stream-maps
    settings:
      - name: api_key
        kind: password
        sensitive: true
      - name: years
        kind: array
      - name: geography_levels
        kind: array
      - name: datasets
        kind: array
      - name: states
        kind: array
      - name: strict_mode
        kind: boolean
    select:
      - county_population.*
      - state_population.*
      - decennial_population.*
      - county_geography.*
    config:
      api_key: ${CENSUS_API_KEY}
      years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
      geography_levels: ["county", "state"]
      datasets: ["pep", "decennial"]
      states: ["*"]

  loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
```

---

## Critical Rules

1. **ALL imports at top of file.** No inline imports.
2. **Census returns 2D arrays, not JSON objects.** Parse accordingly.
3. **All values are strings.** Cast population to int, density/lat/lon to float.
4. **`"null"` is the string "null"**, not JSON null. Handle it.
5. **FIPS codes are zero-padded strings**, not integers. "06" not 6.
6. **County geography (lat/lon centroids)** is essential for joining weather data to population.
7. **Don't over-engineer.** This is ~35K records total. Keep it simple.
8. **PEP variable names include the year.** Build dynamically: `POP_{year}`.
9. **Use UV** for all Python execution.
10. **Match existing tap patterns** from tap-fred, tap-fmp, tap-massive.
