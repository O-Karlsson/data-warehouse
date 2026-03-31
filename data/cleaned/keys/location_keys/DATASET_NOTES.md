# Dataset notes

_Generated on 2026-03-26 15:25:04_

- This dataset is a country-level crosswalk between UN WPP locations and IHME location hierarchy identifiers.
- The base country list comes from `data/cleaned/unwpp/metadata/isoRegions/data.csv`.
- IHME matches are restricted to `level == 3`, which this script treats as country-level IHME locations.
- Matching is done primarily on normalized location names (`neat_location`), with optional manual overrides from `data/manual/keys/location_key_overrides.csv`.
- Additional lookup columns are attached from DHS metadata (`dhs_countrycode`) and the manual CIH region file (`cih_region`).
- `match_method` records whether the IHME match came from exact normalized-name matching or a manual override.
