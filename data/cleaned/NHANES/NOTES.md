# NHANES Notes

_Generated on 2026-03-24 16:31:11_

- The build uses `nhanes_download_plan.csv` as the single source of truth for local folder structure.
- Raw files are downloaded into `data/raw/NHANES` using the `local_folder` and `file_name` columns from that plan.
- Cleaned outputs mirror the same relative folder structure under `data/cleaned/NHANES`.
- Readable raw `XPT` and `CSV` files are converted to `.parquet`, `.dta`, and optional `.csv` sidecars.
- Fixed-width text files are converted when a readable SAS/setup script is available.
- Planned files: 2132.
- Converted files: 1178.
- Conversion failures: 587.
- Download inventory: `data/cleaned/NHANES/download_inventory.csv`.
- Conversion inventory: `data/cleaned/NHANES/conversion_inventory.csv`.
- Download failures: `data/cleaned/NHANES/download_failures.csv`.
- Conversion failures: `data/cleaned/NHANES/conversion_failures.csv`.
