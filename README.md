# Data Warehouse

This repository contains the code, documentation, and metadata for a data warehouse for demographic and global health research.

---

## Repository structure

    data-warehouse/
    ├── R/                     # Code for downloading and light cleaning
    ├── data/                  # Local data (ignored by Git)
    │   ├── raw/               # Original source data
    │   ├── cleaned/           # Datasets (closer to) analysis-ready
    │   ├── manual/            # Simple helper data files
    │   └── unprocessed/       # Data not yet processed
    └── README.md


Data in the `raw/` and `cleaned/` directories are generally organized hierarchically as follows:
1) Data source (e.g., HMD, UNWPP, DHS)
2) Data component (e.g., life tables, population, specific survey module, survey year)
3) Additional subdivisions as applicable (e.g., by country)

Only documentation files inside `data/` are tracked. No `raw/` or `cleaned/` datasets are stored on Github.

# 
---

## Data policy

When possible, code will download raw data automatically (if not already stored locally) into the `raw\` folder either through direct urls, APIs, or web scraping. In some cases, manual download is required due to data use agreements or technical limitations.


**Processing ("cleaning") differs by "type" of data, which can be split into two broad categories: simple aggregate data and complex survey data.**

### Simple aggregate data
- These are e.g., life tables, number of deaths (overall or by cause), population, disease incidence.
- Raw data are only lightly cleaned to make them analysis-ready:
  - The first row contains variable names followed by data.
  - Each metric is stored in a separate column (data are reshaped when needed).
  - Variable names are renamed using standard variable names so datasets are easily combined. E.g., qx, mx, ax, sex (1=male, 2=female, 3=both), age (meaning start of an age interval if more than one year), iso3, year.
- In most cases, a single input file from `raw/` results in a single output file in `cleaned/`.
  - Exceptions include `raw/` files that are:
    - Separated by sex or subnational region, or
    - separated by variables that are very commonly used together.
    - These may be combined in `cleaned/`.
- “Build” scripts are typically organized around each output file in `cleaned/`.
  - In many cases, build scripts from the same data source (e.g. HMD) are very similar, but are kept separate for simplicity (this may be revised at later stages).
- Only information that is very unlikely to be used is removed.
- When data does not include standardized location ids (e.g., iso3), location identifiers are extracted and added to a location key (see build_location_keys.R).
- For larger data, keys for labels (e.g., causes of death) may be extracted and stored separately.

### Complex survey data
- These are e.g., DHS, NHANES, IFLS survey.s
- `cleaned\` contains analysis ready file formats but otherwise there is no cleaning done.
- No information is added or removed.

#
---

## Dataset-level documentation

Dataset directory under `data/raw/` generally include a short text file
(e.g. `SOURCE.md`) describing:
- Where data can be accessed online
- Either date when SOURCE.md was created or data were downloaded
- Coverage and known limitations

Dataset directory under `data/cleaned/` generally  include a `DATASET_NOTES.md`
file describing:
- Date the cleaned dataset was created
- A short description of the dataset
- Source of the original data
- Special considerations or deviations from standard processing

Dataset directory under `data/cleaned/` often also include
`variables_info.csv`, listing:
- Variables (columns) included
- A label for each variable
- Notes on special considerations

---

## Reproducibility

This project uses **renv** to manage R package versions.

To restore the package environment after cloning, run:

    renv::restore()

---

## Notes
- Scripts assume project-relative paths (no hard-coded local directories)
- Documentation is intentionally lightweight and kept close to the data

