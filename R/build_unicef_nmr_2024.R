# R/build_unicef_nmr_2024.R
#
# PURPOSE
# -------
# Download and clean UNICEF neonatal mortality rates (NMR) country estimates.
#
# Input:
#   - Excel file (UNICEF): Neonatal_Mortality_Rates_2024.xlsx
#   - Sheet used: "NMR Country estimates"
#   - First 14 rows are irrelevant; row 15 contains column labels.
#
# Cleaning steps:
#   1) Read sheet, skip first 14 rows.
#   2) Data are wide by year (e.g., "1952.5" columns). Reshape wide -> long:
#        - years become rows
#        - values become `nmr`
#        - create `year` = floor(year_label)
#   3) Reshape again long -> wide by uncertainty bounds:
#        Uncertainty.Bounds* (Lower/Median/Upper) -> nmr_l / nmr / nmr_u
#   4) Rename:
#        ISO.Code -> iso3
#        Country.Name -> location
#
# Final columns:
#   iso3, location, year, nmr_l, nmr, nmr_u
#
# Raw storage:
#   data/raw/unicef/neonatal mortality rates/
#
# Cleaned storage:
#   data/cleaned/unicef/neonatal mortality rates/
#
# Outputs:
#   - data.parquet (primary)
#   - data.dta
#   - data.csv.gz
#
# Source:
#   https://data.unicef.org/wp-content/uploads/2025/03/Neonatal_Mortality_Rates_2024.xlsx

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readxl)
  library(arrow)
  library(haven)
  library(readr)
  library(stringr)
})

URL_NMR <- "https://data.unicef.org/wp-content/uploads/2025/03/Neonatal_Mortality_Rates_2024.xlsx?client_id=685685579.1771430808&session_id=704012225"
SHEET   <- "NMR Country estimates"

base_raw   <- file.path("data", "raw", "unicef", "neonatal mortality rates")
base_clean <- file.path("data", "cleaned", "unicef", "neonatal mortality rates")
dir.create(base_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(base_clean, recursive = TRUE, showWarnings = FALSE)

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
# 1) Download raw
# ------------------------------------------------------------------------------
raw_file <- file.path(base_raw, "Neonatal_Mortality_Rates_2024.xlsx")
download_if_needed(URL_NMR, raw_file)

# ------------------------------------------------------------------------------
# 2) Read + reshape wide -> long (years in columns)
# ------------------------------------------------------------------------------
raw_df <- readxl::read_excel(
  path        = raw_file,
  sheet       = SHEET,
  skip        = 14,              # first 14 rows irrelevant; row 15 is header
  .name_repair = "minimal"
)

# Identify year columns like "1952.5" (or "1952", etc.)
year_cols <- names(raw_df)[stringr::str_detect(names(raw_df), "^\\d{4}(?:\\.\\d+)?$")]

if (length(year_cols) == 0) {
  stop("No year columns detected (expected columns named like 1952.5).", call. = FALSE)
}

# Keep only what we need for reshaping
df_long <- raw_df %>%
  select(
    `ISO.Code`,
    `Country.Name`,
    `Uncertainty.Bounds*`,
    all_of(year_cols)
  ) %>%
  pivot_longer(
    cols      = all_of(year_cols),
    names_to  = "year_label",
    values_to = "nmr"
  ) %>%
  mutate(
    year = suppressWarnings(floor(as.numeric(year_label)))
  ) %>%
  select(-year_label)

# ------------------------------------------------------------------------------
# 3) Reshape long -> wide by uncertainty bounds
# ------------------------------------------------------------------------------
df_clean <- df_long %>%
  mutate(
    bound = dplyr::case_when(
      `Uncertainty.Bounds*` == "Lower"  ~ "l",
      `Uncertainty.Bounds*` == "Median" ~ "m",
      `Uncertainty.Bounds*` == "Upper"  ~ "u",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(bound), !is.na(year)) %>%
  select(`ISO.Code`, `Country.Name`, year, bound, nmr) %>%
  # In case there are duplicates, keep the first non-missing value per cell
  group_by(`ISO.Code`, `Country.Name`, year, bound) %>%
  summarise(nmr = dplyr::first(nmr[!is.na(nmr)]), .groups = "drop") %>%
  pivot_wider(
    names_from   = bound,
    values_from  = nmr,
    names_prefix = "nmr_"
  ) %>%
  rename(
    iso3     = `ISO.Code`,
    location = `Country.Name`,
    nmr      = nmr_m
  ) %>%
  select(iso3, location, year, nmr_l, nmr, nmr_u) %>%
  arrange(iso3, year)

# ------------------------------------------------------------------------------
# 4) Write outputs (archive + promote pattern)
# ------------------------------------------------------------------------------
arch_dir <- file.path(base_clean, "_archive")
dir.create(arch_dir, recursive = TRUE, showWarnings = FALSE)

out_parquet_final <- file.path(base_clean, "data.parquet")
out_parquet_tmp   <- tempfile(fileext = ".parquet")
arrow::write_parquet(df_clean, out_parquet_tmp)

res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  if (file.exists(file.path(base_clean, "data.dta"))) {
    file.copy(
      file.path(base_clean, "data.dta"),
      file.path(arch_dir, paste0("data_", tag, ".dta")),
      overwrite = FALSE
    )
  }
  if (file.exists(file.path(base_clean, "data.csv.gz"))) {
    file.copy(
      file.path(base_clean, "data.csv.gz"),
      file.path(arch_dir, paste0("data_", tag, ".csv.gz")),
      overwrite = FALSE
    )
  }
}

haven::write_dta(df_clean, file.path(base_clean, "data.dta"))
readr::write_csv(df_clean, file.path(base_clean, "data.csv.gz"))

# ------------------------------------------------------------------------------
# Documentation
# ------------------------------------------------------------------------------

# Write SOURCES.md to keep with raw data
sources_md <- file.path(base_raw, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Source: UNICEF Data (Neonatal Mortality Rates 2024).",
      paste0("Access point: ", URL_NMR),
      paste0("Sheet used: ", SHEET),
      "Notes: First 14 rows skipped; row 15 treated as column labels.",
      paste0("This file was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# Write dataset notes with cleaned data
dataset_notes <- c(
  "This dataset contains country-level neonatal mortality rate (NMR) estimates from UNICEF.",
  "The raw Excel sheet is wide by year (year columns named like 1952.5).",
  "Year labels are converted to integer calendar years using floor() (e.g., 1952.5 -> 1952).",
  "Values are reshaped to long format and then widened by uncertainty bounds.",
  "Uncertainty bounds produce three columns: nmr_l (Lower), nmr (Median), nmr_u (Upper).",
  "Only iso3, location, year, nmr_l, nmr, nmr_u are retained in the final output."
)

notes_path <- file.path(base_clean, "DATASET_NOTES.md")
notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("- ", dataset_notes)
)
writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# Variable dictionary
var_dict <- data.frame(
  var = c("iso3", "location", "year", "nmr_l", "nmr", "nmr_u"),
  var_label = c(
    "ISO3 country code",
    "Country name",
    "Calendar year (floor of original year label)",
    "Neonatal mortality rate (lower uncertainty bound)",
    "Neonatal mortality rate (median estimate)",
    "Neonatal mortality rate (upper uncertainty bound)"
  ),
  notes = c(
    "Renamed from ISO.Code.",
    "Renamed from Country.Name.",
    "Original year columns are labeled like 1952.5; converted via floor().",
    "Derived from Uncertainty.Bounds* == Lower.",
    "Derived from Uncertainty.Bounds* == Median.",
    "Derived from Uncertainty.Bounds* == Upper."
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(base_clean, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

# Emit documentation warnings at the very end
doc_warnings <- doc_warnings_for_var_dict(
  df_clean,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "df_clean"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}
