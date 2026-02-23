# Dataset notes

_Generated on 2026-02-20 12:20:11_

- This dataset combines UN WPP 2024 population counts, deaths, and population exposure by location, year, and single-year age.
- Within each period (estimates vs projections), population, deaths, and exposure files are downloaded and joined on LocID, Time, and AgeGrpStart.
- The joined data are reshaped from wide sex-specific columns to long format.
- Sex-specific columns PopMale/PopFemale/PopTotal (population), DeathMale/DeathFemale/DeathTotal (deaths), and PopMale/PopFemale/PopTotal (exposure) are converted to rows with variables pop, deaths, exposure and an integer sex code (1=Male, 2=Female, 3=Total).
- Minimal harmonization is performed: ISO3_code->iso3, Time->year, AgeGrpStart->age.
- Estimates and projections are stored separately and are not combined in the cleaned outputs.
- Units/definitions (e.g., mid-year population vs period exposure; deaths definition) follow UN WPP documentation and may differ across WPP vintages.
