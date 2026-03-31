# Dataset notes

_Generated on 2026-03-26 10:06:50_

- This dataset combines 9 Our World in Data grapher extracts for academic performance.
- Subjects included: mathematics, science, and reading.
- Sex breakdowns included: both, girls, and boys.
- Raw CSV and metadata JSON files are stored in `data/raw/OWID/academic-performance/`.
- The cleaned dataset is created by full-joining all source CSV files on `entity`, `code`, and `year`.
- Value columns are renamed to standardized warehouse-style names such as `score_math_both` and `score_reading_girls`.
- Source URLs are parameterized OWID grapher download links supplied directly in the build script.
