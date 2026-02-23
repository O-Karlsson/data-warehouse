# Dataset notes

_Generated on 2026-02-20 16:29:25_

- This dataset contains two all-cause number of deaths from IHME GBD results, reshaped to wide format:
-   - dth1: age_name == '<1 year' (number deaths before age 1 year)
-   - dthn: age_name == '<28 days' (number of deaths before age 28 days)
- Data were manually obtained from the Institute for Health Metrics and Evaluation (IHME) Global Burden of Disease (GBD) Results tool:
- https://vizhub.healthdata.org/gbd-results/ (requires login).
- Coverage: 204 unique locations; 44 unique years (1980–2023).
- Sex coding follows IHME: 1=male, 2=female, 3=both.
- If either dth1 or dthn is missing for a row, the corresponding outcome was not present in the raw export for that key.
