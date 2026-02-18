# Dataset notes

_Generated on 2026-02-18 15:47:19_

- This dataset contains country-level neonatal mortality rate (NMR) estimates from UNICEF.
- The raw Excel sheet is wide by year (year columns named like 1952.5).
- Year labels are converted to integer calendar years using floor() (e.g., 1952.5 -> 1952).
- Values are reshaped to long format and then widened by uncertainty bounds.
- Uncertainty bounds produce three columns: nmr_l (Lower), nmr (Median), nmr_u (Upper).
- Only iso3, location, year, nmr_l, nmr, nmr_u are retained in the final output.
