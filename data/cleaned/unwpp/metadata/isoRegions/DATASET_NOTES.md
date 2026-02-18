# Dataset notes

_Generated on 2026-02-18 14:29:20_

- This dataset is a simplified ISO3-to-region lookup derived from UN WPP 2024 locations metadata (Aggregation_Lists sheet).
- Rows are restricted to ParentTypeName in {Region, Subregion, Income Group}.
- Rows are excluded when ParentPrintName is one of: High-and-upper-middle-income countries; Low-and-Lower-middle-income countries; Low-and-middle-income countries; Middle-income countries.
- Data are reshaped wide so there is one row per iso3 with columns: region, subregion, incomegr.
