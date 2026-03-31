# NHANES Notes

_Generated on 2026-03-31 11:44:29_

- The build uses `data/NHANES links All.csv` as the source of truth.
- Raw assets are downloaded into `data/raw/NHANES/<wave>`.
- Cleaned outputs mirror the same relative structure under `data/cleaned/NHANES/<wave>`.
- The script downloads data files, SAS/setup files, and formatted SAS files, excluding FTP-style folder entries.
- Readable raw `XPT` and `CSV` files are converted directly to `.parquet`, `.dta`, and optional `.csv` sidecars.
- Fixed-width `TXT` and `DAT` files are converted using the SAS/setup file from the same source row whenever available.
- Source CSV rows: 1845.
- Downloadable assets: 2001.
- Converted files: 1833.
- Conversion failures: 1.
- Download inventory: `data/cleaned/NHANES/download_inventory.csv`.
- Conversion inventory: `data/cleaned/NHANES/conversion_inventory.csv`.
- Download failures: `data/cleaned/NHANES/download_failures.csv`.
- Conversion failures: `data/cleaned/NHANES/conversion_failures.csv`.
- Filename conflicts: `data/cleaned/NHANES/filename_conflicts.csv`.
- Asset manifest: `data/cleaned/NHANES/nhanes_asset_manifest.csv`.
- Row manifest: `data/cleaned/NHANES/nhanes_row_manifest.csv`.
