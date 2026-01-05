# Data Warehouse

This repository contains the **code, documentation, and metadata** for a reproducible data warehouse used in demographic and global health research.

The repository is **code-only by design**.  
Raw and processed datasets are stored locally and are intentionally excluded from version control.

---

## Repository structure

    data-warehouse/
    ├── R/                     # Code for light cleaning and core functions
    ├── data/                  # Local data (ignored by Git)
    │   ├── raw/               # Original source data
    │   └── cleaned/           # Analysis-ready datasets
    ├── renv.lock              # Reproducible R environment
    ├── data-warehouse.Rproj   # RStudio project
    └── README.md

Only documentation files (e.g. `.md`) inside `data/` are tracked.

`raw\` data and `cleaned\` data are organized by **data source (eg, HMD, UNWPP) -> type of data (eg, life tables, population) -> country or region (if applicable)**.

---

## Data policy

- No `raw/` or `cleaned/` datasets are stored in this repository.
- Raw data are **only lightly cleaned** to make them analysis-ready:
  - The first row contains variable names followed by data.
  - Each metric is stored in a separate column (data are reshaped when needed).
  - In most cases, a single input file from `raw/` results in a single output file in `cleaned/`.
  - Exceptions include files that are:
    - separated by sex or subnational region, or
    - variables that are very commonly used together in `raw/`,
      which may be combined in `cleaned/`.
  - Only information that is very unlikely to be used is removed.
- When possible, code will download data automatically if not stored locally.
- “Build” scripts are typically organized around each output file in `cleaned/`:
  - In many cases, build scripts from the same data source (e.g. HMD) are very similar,
    but are kept separate for simplicity (this may be revised at later stages).

---

## Dataset-level documentation

Each dataset directory under `data/raw/` should include a short provenance file
(e.g. `SOURCE.md`) describing:

- Data provider
- Version and access date
- Source URL
- Download method
- Coverage and known limitations

Each dataset directory under `data/cleaned/` should include a `DATASET_NOTES.md`
file describing:

- Date the cleaned dataset was created
- A short description of the dataset
- Source of the original data
- Special considerations or deviations from standard processing

Each dataset directory under `data/cleaned/` should also include
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

## Getting started

1. Clone the repository:

       git clone https://github.com/O-Karlsson/data-warehouse.git

2. Open `data-warehouse.Rproj` in RStudio
3. Restore packages with `renv::restore()`
4. Manually download data when required

---

## Notes

- Scripts assume project-relative paths (no hard-coded local directories)
- Documentation is intentionally lightweight and kept close to the data

