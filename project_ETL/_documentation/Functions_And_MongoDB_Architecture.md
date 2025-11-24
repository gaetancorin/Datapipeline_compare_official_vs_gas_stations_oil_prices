# Functions And MongoDB Architecture

## Architecture of Functions and APIs

- /etl/launch_complete_pipeline_oils_prices
  - /etl/launch_etl_gas_stations_oils_prices
    - determine_dates_to_load_from_mongo()
    - extract_api_gas_stations_oils_prices()
    - transform_gas_stations_oils_prices()
    - load_gas_stations_oils_prices_to_mongo()
  - /etl/launch_etl_denormalize_stations_prices
    - determine_dates_to_load_from_mongo()
    - extract_stations_prices_from_mongo()
    - transform_and_denormalize_stations_prices()
    - load_denormalized_stations_prices_to_mongo()
  - /etl/launch_etl_denormalize_official_oils_prices
    - determine_dates_to_load_from_mongo()
    - extract_api_denorm_official_oils_prices()
    - transform_denorm_official_oils_prices()
    - load_denorm_official_oils_prices_to_mongo()
  - /etl/merge_denorm_stations_vs_official_prices
    - determine_dates_to_load_from_mongo()
    - extract_denorm_stations_prices_from_mongo()
    - extract_official_oils_prices_from_mongo()
    - transform_merge_stations_vs_official_prices()
    - load_denorm_stations_vs_official_prices_to_mongo()

---

# MongoDB Structure

## Databases and Collections

### `datalake`
- `gas_stations_infos`
- `gas_stations_prices`

### `denormalization`
- `denorm_stations_prices`
- `denorm_official_prices`
- `denorm_station_vs_official_prices`

---

### Data Count (if all data loaded from 2007 to 2025)
- `gas_stations_prices`: 39,513,801 elements
- `gas_stations_infos`: 13,660 elements
- `denorm_stations_prices`: 6,749 elements
- `denorm_official_prices`: 2,112 elements
- `denorm_station_vs_official_prices`: 7,897 elements

**Total loading time:** 1h25

**CSV file size:** 56,651,223 rows