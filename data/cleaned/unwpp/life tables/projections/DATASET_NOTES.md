# Dataset notes

_Generated on 2026-02-18 11:31:09_

- This dataset contains UN WPP 2024 period life table quantities by location, year, sex, and age (single-year age groups).
- The build script downloads multiple CSV.GZ source files and appends them within each period (estimates vs projections).
- Only a subset of life table variables is retained (mx, qx, ex, ax) together with identifiers (LocID, iso3, year, sex, age).
- Minimal harmonization is performed: ISO3_code->iso3, Time->year, SexID->sex, AgeGrpStart->age.
- Estimates and projections are stored in separate folders and are not combined in the cleaned outputs.
- Check UN WPP technical documentation for definitions/units and any revisions to methods across WPP vintages.
