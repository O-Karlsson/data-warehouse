# Dataset notes

_Generated on 2026-02-18 11:45:06_

- This dataset is derived from UN WPP 2024 Demographic Indicators (Medium scenario).
- The notes file (WPP2024_Demographic_Indicators_notes.csv) is stored in both raw and cleaned folders.
- Cleaning performed:
  - Dropped columns: SortOrder, LocID, Notes, ISO3_code, ISO2_code, SDMX_code, LocTypeID, LocTypeName, ParentID, Location, VarID, Variant.
  - Renamed ISO3_code -> iso3 and Time -> year.
- All remaining columns are kept as provided (indicator columns by year/location).
