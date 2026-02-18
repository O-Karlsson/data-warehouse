# R/build_unwpp_demovars.R
#
# PURPOSE
# -------
# Download and lightly clean UN WPP 2024 Demographic Indicators (Medium scenario)
# and store as a standardized dataset for downstream use.
#
# Inputs:
#   - WPP2024_Demographic_Indicators_Medium.csv.gz   (main data)
#   - WPP2024_Demographic_Indicators_notes.csv      (column/variable notes)
#
# Raw stored in:
#   data/raw/unwpp/demovars/
#
# Cleaned stored in:
#   data/cleaned/unwpp/demovars/
#
# Cleaning:
#   - Drop columns:
#       SortOrder, LocID, Notes, ISO3_code, ISO2_code, SDMX_code,
#       LocTypeID, LocTypeName, ParentID, Location, VarID, Variant
#   - Rename:
#       ISO3_code -> iso3
#       time      -> year
#
# Outputs:
#   - data.parquet (primary)
#   - data.dta
#   - data.csv
#   - notes file copied to BOTH raw and cleaned folders
#
# Sources:
#   https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz
#   https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_notes.csv

source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
  library(haven)
})

# ------------------------------------------------------------------------------
# URLs
# ------------------------------------------------------------------------------
URL_DATA  <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz"
URL_NOTES <- "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_notes.csv"

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
download_if_needed <- function(url, dest) {
  if (file.exists(dest) && isTRUE(file.info(dest)$size > 0)) return(invisible(TRUE))
  
  ok <- tryCatch({
    suppressWarnings(utils::download.file(url, dest, mode = "wb", quiet = TRUE))
    file.exists(dest) && isTRUE(file.info(dest)$size > 0)
  }, error = function(e) FALSE)
  
  if (!ok) {
    if (file.exists(dest)) unlink(dest)
    stop(sprintf("Download failed: %s -> %s", url, dest), call. = FALSE)
  }
  invisible(TRUE)
}

# ------------------------------------------------------------------------------
# Directory setup (folder name requested: demovars)
# ------------------------------------------------------------------------------
RAW_DIR <- file.path("data", "raw", "unwpp", "demovars")
OUT_DIR <- file.path("data", "cleaned", "unwpp", "demovars")

dir.create(RAW_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Download raw files
# ------------------------------------------------------------------------------
raw_data_path  <- file.path(RAW_DIR, "WPP2024_Demographic_Indicators_Medium.csv.gz")
raw_notes_path <- file.path(RAW_DIR, "WPP2024_Demographic_Indicators_notes.csv")

download_if_needed(URL_DATA,  raw_data_path)
download_if_needed(URL_NOTES, raw_notes_path)

# Also store notes in cleaned folder (as requested)
clean_notes_path <- file.path(OUT_DIR, "WPP2024_Demographic_Indicators_notes.csv")
file.copy(raw_notes_path, clean_notes_path, overwrite = TRUE)

# ------------------------------------------------------------------------------
# Read + clean
# ------------------------------------------------------------------------------
# readr can read .csv.gz directly
df <- readr::read_csv(raw_data_path, show_col_types = FALSE, progress = FALSE)

drop_cols <- c(
  "SortOrder", "Notes", "ISO2_code", "SDMX_code",
  "LocTypeID", "LocTypeName", "ParentID", "Location", "VarID", "Variant"
)

# Only drop columns that actually exist (robust to minor schema changes)
drop_cols_present <- intersect(names(df), drop_cols)

df_clean <- df %>%
  rename(
    iso3 = ISO3_code,
    year = Time
  ) %>%
  select(-all_of(drop_cols_present))

# ------------------------------------------------------------------------------
# Write outputs (archive + promote)
# ------------------------------------------------------------------------------
arch_dir <- file.path(OUT_DIR, "_archive")
dir.create(arch_dir, recursive = TRUE, showWarnings = FALSE)

out_parquet_final <- file.path(OUT_DIR, "data.parquet")
out_parquet_tmp   <- tempfile(fileext = ".parquet")
arrow::write_parquet(df_clean, out_parquet_tmp)

res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

# Always write CSV/DTA (and archive previous versions when parquet changed)
if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  if (file.exists(file.path(OUT_DIR, "data.dta"))) {
    file.copy(
      file.path(OUT_DIR, "data.dta"),
      file.path(arch_dir, paste0("data_", tag, ".dta")),
      overwrite = FALSE
    )
  }
  if (file.exists(file.path(OUT_DIR, "data.csv"))) {
    file.copy(
      file.path(OUT_DIR, "data.csv"),
      file.path(arch_dir, paste0("data_", tag, ".csv")),
      overwrite = FALSE
    )
  }
}

haven::write_dta(df_clean, file.path(OUT_DIR, "data.dta"))
readr::write_csv(df_clean, file.path(OUT_DIR, "data.csv"))

# ------------------------------------------------------------------------------
# SOURCES.md (raw)
# ------------------------------------------------------------------------------
sources_md <- file.path(RAW_DIR, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Source: United Nations, Department of Economic and Social Affairs, Population Division.",
      "Dataset: World Population Prospects 2024 (WPP 2024) - Demographic Indicators (Standard) - Medium scenario.",
      "Main file: WPP2024_Demographic_Indicators_Medium.csv.gz",
      "Notes file: WPP2024_Demographic_Indicators_notes.csv",
      "Access point: https://population.un.org/wpp/",
      "Downloaded from: https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/",
      paste0("This file was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# ------------------------------------------------------------------------------
# Dataset notes (cleaned)
# ------------------------------------------------------------------------------
notes_path <- file.path(OUT_DIR, "DATASET_NOTES.md")
writeLines(
  c(
    "# Dataset notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    "- This dataset is derived from UN WPP 2024 Demographic Indicators (Medium scenario).",
    "- The notes file (WPP2024_Demographic_Indicators_notes.csv) is stored in both raw and cleaned folders.",
    "- Cleaning performed:",
    "  - Dropped columns: SortOrder, LocID, Notes, ISO3_code, ISO2_code, SDMX_code, LocTypeID, LocTypeName, ParentID, Location, VarID, Variant.",
    "  - Renamed ISO3_code -> iso3 and Time -> year.",
    "- All remaining columns are kept as provided (indicator columns by year/location)."
  ),
  notes_path
)
message("Wrote dataset notes: ", notes_path)
