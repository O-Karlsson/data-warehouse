# Dataset notes

_Generated on 2026-02-18 11:50:40_

- This dataset combines UN WPP 2024 population counts and deaths by location, year, and single-year age.
- Within each period (estimates vs projections), a population file and a deaths file are downloaded and joined on LocID, Time, and AgeGrpStart.
- The joined data are reshaped from wide sex-specific columns to long format.
- Sex-specific columns PopMale/PopFemale/PopTotal and DeathMale/DeathFemale/DeathTotal are converted to rows with variables pop and deaths and an integer sex code (1=Male, 2=Female, 3=Total).
- Minimal harmonization is performed: ISO3_code->iso3, Time->year, AgeGrpStart->age.
- Estimates and projections are stored separately and are not combined in the cleaned outputs.
- Units/definitions (e.g., mid-year population vs period exposure; deaths definition) follow UN WPP documentation and may differ across WPP vintages.
