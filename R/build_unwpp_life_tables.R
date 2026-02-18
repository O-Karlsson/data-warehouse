# R/build_unwpp_life_tables.R
#
# PURPOSE
# -------
# Download, append, and clean UN WPP 2024 life table CSV.GZ files for:
#   - estimates   (1950–2023)
#   - projections (2024–2100)
#
# Raw files are stored in:
#   data/raw/unwpp/life tables/<estimates|projections>/
#
# Cleaned outputs are written to:
#   data/cleaned/unwpp/life tables/<estimates|projections>/
#
# Cleaned variables kept:
#   LocID ISO3_code Time SexID AgeGrpStart mx qx ex ax
#
# Renames in cleaned output:
#   ISO3_code   -> iso3
#   SexID       -> sex
#   Time        -> year
#   AgeGrpStart -> age
#
# Output format:
#   - data.parquet (primary, efficient)
#   - data.dta
#   - data.csv  (compressed csv)
#
# Sources (UN WPP 2024):
#   Estimates:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Both_1950-2023.csv.gz
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv.gz
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv.gz
#   Projections:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Both_2024-2100.csv.gz
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv.gz
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv.gz

# ------------------------------------------------------------------------------
# Global constants / helpers
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(vroom)
  library(dplyr)
  library(arrow)
  library(haven)
  library(readr)
})

KEEP_COLS <- c("LocID", "ISO3_code", "Time", "SexID", "AgeGrpStart", "mx", "qx", "ex", "ax")

COL_TYPES <- vroom::cols(
  LocID       = vroom::col_integer(),
  ISO3_code   = vroom::col_character(),
  Time        = vroom::col_integer(),
  SexID       = vroom::col_integer(),
  AgeGrpStart = vroom::col_integer(),
  mx          = vroom::col_double(),
  qx          = vroom::col_double(),
  ex          = vroom::col_double(),
  ax          = vroom::col_double(),
  .default    = vroom::col_guess()
)

URLS <- list(
  estimates = c(
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Both_1950-2023.csv.gz",
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_1950-2023.csv.gz",
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_1950-2023.csv.gz"
  ),
  projections = c(
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Both_2024-2100.csv.gz",
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Female_2024-2100.csv.gz",
    "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Life_Table_Complete_Medium_Male_2024-2100.csv.gz"
  )
)

# ------------------------------------------------------------------------------
# Directory setup
# ------------------------------------------------------------------------------
base_raw   <- file.path("data", "raw", "unwpp", "life tables")
base_clean <- file.path("data", "cleaned", "unwpp", "life tables")

dir.create(base_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(base_clean, recursive = TRUE, showWarnings = FALSE)

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

read_one_gz <- function(path) {
  vroom::vroom(
    file       = path,
    delim      = ",",
    col_select = any_of(KEEP_COLS),
    col_types  = COL_TYPES,
    progress   = FALSE,
    altrep     = TRUE
  )
}

# ------------------------------------------------------------------------------
# Build (estimates + projections)
# ------------------------------------------------------------------------------
for (period in names(URLS)) {
  
  dir_raw   <- file.path(base_raw,   period)
  dir_clean <- file.path(base_clean, period)
  dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  urls <- URLS[[period]]
  
  # 1) Download raw .csv.gz files
  raw_paths <- vapply(urls, function(u) {
    fn <- basename(utils::URLdecode(u))
    fp <- file.path(dir_raw, fn)
    download_if_needed(u, fp)
    fp
  }, FUN.VALUE = character(1))
  
  # 2) Read + append (no reshaping)
  df <- bind_rows(lapply(raw_paths, read_one_gz))
  
  # 3) Keep + rename columns (minimal changes)
  df_clean <- df %>%
    select(all_of(KEEP_COLS)) %>%
    rename(
      iso3 = ISO3_code,
      sex  = SexID,
      year = Time,
      age  = AgeGrpStart
    )
  
  # 4) Write outputs (with archiving pattern like other build scripts)
  arch_dir <- file.path(dir_clean, "_archive")
  dir.create(arch_dir, recursive = TRUE, showWarnings = FALSE)
  
  out_parquet_final <- file.path(dir_clean, "data.parquet")
  out_parquet_tmp   <- tempfile(fileext = ".parquet")
  arrow::write_parquet(df_clean, out_parquet_tmp)
  
  res <- archive_and_promote(
    final_path  = out_parquet_final,
    tmp_path    = out_parquet_tmp,
    archive_dir = arch_dir
  )
  
  if (isTRUE(res$changed)) {
    tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
    
    # Archive previous sidecar outputs if present
    if (file.exists(file.path(dir_clean, "data.dta"))) {
      file.copy(
        file.path(dir_clean, "data.dta"),
        file.path(arch_dir, paste0("data_", tag, ".dta")),
        overwrite = FALSE
      )
    }
    if (file.exists(file.path(dir_clean, "data.csv.gz"))) {
      file.copy(
        file.path(dir_clean, "data.csv.gz"),
        file.path(arch_dir, paste0("data_", tag, ".csv.gz")),
        overwrite = FALSE
      )
    }
  }
  
  haven::write_dta(df_clean, file.path(dir_clean, "data.dta"))
  readr::write_csv(df_clean, file.path(dir_clean, "data.csv.gz"))
}


# ------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------

# ------------------------------------------------------------------------------
# Write SOURCES.md to keep with raw data
# ------------------------------------------------------------------------------

sources_md <- file.path(dir_raw, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Source: United Nations, Department of Economic and Social Affairs, Population Division.",
      "Dataset: World Population Prospects 2024 (WPP 2024) life tables (Complete, Medium).",
      "Access point: https://population.un.org/wpp/",
      "Files downloaded from: https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/",
      paste0("This file was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# ------------------------------------------------------------------------------
# Write info to keep with cleaned data
# ------------------------------------------------------------------------------

dataset_notes <- c(
  "This dataset contains UN WPP 2024 period life table quantities by location, year, sex, and age (single-year age groups).",
  "The build script downloads multiple CSV.GZ source files and appends them within each period (estimates vs projections).",
  "Only a subset of life table variables is retained (mx, qx, ex, ax) together with identifiers (LocID, iso3, year, sex, age).",
  "Minimal harmonization is performed: ISO3_code->iso3, Time->year, SexID->sex, AgeGrpStart->age.",
  "Estimates and projections are stored in separate folders and are not combined in the cleaned outputs.",
  "Check UN WPP technical documentation for definitions/units and any revisions to methods across WPP vintages."
)

notes_path <- file.path(dir_clean, "DATASET_NOTES.md")
notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("- ", dataset_notes)
)
writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------
# Variable dictionary (edit var_label / notes here)
# ------------------------------------------------------------

var_dict <- data.frame(
  var = c("LocID", "iso3", "year", "sex", "age", "mx", "qx", "ex", "ax"),
  var_label = c(
    "UN WPP location identifier",
    "ISO3 country code",
    "Calendar year",
    "Sex identifier",
    "Age (years), single-year age group start",
    "Age-specific central death rate (mx)",
    "Age-specific probability of dying between age x and x+1 (qx)",
    "Remaining life expectancy at exact age x (ex)",
    "Average number of person-years lived by those dying between x and x+1 (ax)"
  ),
  notes = c(
    "",
    "",
    "",
    "1=Male, 2=Female, 3=Total (as provided by UN WPP files used here).",
    "Single-year age; interpreted as start of age interval.",
    "See UN WPP documentation for units/definitions and any age-interval conventions.",
    "",
    "",
    ""
  ),
  stringsAsFactors = FALSE
)

# ------------------------------------------------------------
# Write variable dictionary
# ------------------------------------------------------------

vars_path <- file.path(dir_clean, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------
# Emit documentation warnings at the very end
# ------------------------------------------------------------

doc_warnings <- doc_warnings_for_var_dict(
  df_clean,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "df_clean"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}

