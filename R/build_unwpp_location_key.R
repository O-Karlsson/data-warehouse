# R/build_unwpp_location_key.R
#
# PURPOSE
# -------
# Download UN WPP 2024 locations metadata Excel and build a key for UN location codes.
#
# Input:
#   - Excel file (WPP2024_F01_LOCATIONS.xlsx)
#   - Sheet: "Aggregation_Lists"
#
# Raw file stored in:
#   data/raw/unwpp/metadata/
#
# Cleaned outputs written to:
#   1) data/cleaned/unwpp/metadata/location_key/
#      - Minimal harmonization (all columns retained)
#   2) data/cleaned/unwpp/metadata/isoRegions/
#      - Simplified ISO3 -> region/subregion/income group lookup
#
# Minimal harmonization (location_key):
#   - Keep all columns
#   - Rename:
#       LocPrintName -> location
#       ISO3Code     -> iso3
#
# Source:
#   https://population.un.org/wpp/assets/Excel%20Files/4_Metadata/WPP2024_F01_LOCATIONS.xlsx

# ------------------------------------------------------------------------------
# Global constants / helpers
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(haven)
  library(readr)
})

URL <- "https://population.un.org/wpp/assets/Excel%20Files/4_Metadata/WPP2024_F01_LOCATIONS.xlsx"

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
# Directory setup
# ------------------------------------------------------------------------------
RAW_DIR      <- file.path("data", "raw", "unwpp", "metadata")
OUT_DIR      <- file.path("data", "cleaned", "unwpp", "metadata", "location_key")
OUT_DIR_ISO  <- file.path("data", "cleaned", "unwpp", "metadata", "isoRegions")

dir.create(RAW_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR_ISO, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Download raw Excel
# ------------------------------------------------------------------------------
raw_path <- file.path(RAW_DIR, "WPP2024_F01_LOCATIONS.xlsx")
download_if_needed(URL, raw_path)

# ------------------------------------------------------------------------------
# Read + minimal harmonization
# ------------------------------------------------------------------------------
df <- readxl::read_excel(raw_path, sheet = "Aggregation_Lists")

df_clean <- df %>%
  rename(
    location = LocPrintName,
    iso3     = ISO3Code
  )

# ------------------------------------------------------------------------------
# Write outputs (location_key) (archive + promote like other build scripts)
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
# Create simplified ISO3 -> (region, subregion, incomegr) lookup
# ------------------------------------------------------------------------------
keep_parent_types <- c("Region", "Subregion", "Income Group")
drop_parent_print <- c(
  "High-and-upper-middle-income countries",
  "Low-and-Lower-middle-income countries",
  "Low-and-middle-income countries",
  "Middle-income countries"
)

df_iso_regions <- df_clean %>%
  filter(ParentTypeName %in% keep_parent_types) %>%
  filter(!(ParentPrintName %in% drop_parent_print)) %>%
  filter(!is.na(iso3), trimws(iso3) != "") %>%
  transmute(
    iso3        = trimws(iso3),
    location    = location,
    parent_col  = recode(
      ParentTypeName,
      "Region"       = "region",
      "Subregion"    = "subregion",
      "Income Group" = "incomegr",
      .default = NA_character_
    ),
    parent_value = ParentPrintName
  ) %>%
  filter(!is.na(parent_col)) %>%
  group_by(iso3, location, parent_col) %>%
  summarise(parent_value = dplyr::first(na.omit(parent_value)), .groups = "drop") %>%
  tidyr::pivot_wider(
    names_from  = parent_col,
    values_from = parent_value,
    values_fn   = dplyr::first,
    values_fill = NA_character_
  ) %>%
  mutate(
    subregion = dplyr::coalesce(subregion, region),
    incomegr  = dplyr::coalesce(incomegr, "No income group available")
  ) %>%
  select(iso3, location, region, subregion, incomegr) %>%
  arrange(iso3)

# ------------------------------------------------------------------------------
# Write simplified ISO regions outputs (archive + promote)
# ------------------------------------------------------------------------------
arch_dir_iso <- file.path(OUT_DIR_ISO, "_archive")
dir.create(arch_dir_iso, recursive = TRUE, showWarnings = FALSE)

out_parquet_iso_final <- file.path(OUT_DIR_ISO, "data.parquet")
out_parquet_iso_tmp   <- tempfile(fileext = ".parquet")
arrow::write_parquet(df_iso_regions, out_parquet_iso_tmp)

res_iso <- archive_and_promote(
  final_path  = out_parquet_iso_final,
  tmp_path    = out_parquet_iso_tmp,
  archive_dir = arch_dir_iso
)

if (isTRUE(res_iso$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  if (file.exists(file.path(OUT_DIR_ISO, "data.dta"))) {
    file.copy(
      file.path(OUT_DIR_ISO, "data.dta"),
      file.path(arch_dir_iso, paste0("data_", tag, ".dta")),
      overwrite = FALSE
    )
  }
  if (file.exists(file.path(OUT_DIR_ISO, "data.csv"))) {
    file.copy(
      file.path(OUT_DIR_ISO, "data.csv"),
      file.path(arch_dir_iso, paste0("data_", tag, ".csv")),
      overwrite = FALSE
    )
  }
}

haven::write_dta(df_iso_regions, file.path(OUT_DIR_ISO, "data.dta"))
readr::write_csv(df_iso_regions, file.path(OUT_DIR_ISO, "data.csv"))

# ------------------------------------------------------------------------------
# SOURCES.md (kept with raw data)
# ------------------------------------------------------------------------------
sources_md <- file.path(RAW_DIR, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Source: United Nations, Department of Economic and Social Affairs, Population Division.",
      "Dataset: World Population Prospects 2024 (WPP 2024) metadata.",
      "File: WPP2024_F01_LOCATIONS.xlsx",
      "Access point: https://population.un.org/wpp/",
      "Downloaded from: https://population.un.org/wpp/assets/Excel%20Files/4_Metadata/",
      paste0("This file was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# ------------------------------------------------------------------------------
# Dataset notes (kept with cleaned data) - location_key
# ------------------------------------------------------------------------------
dataset_notes <- c(
  "This dataset is a key derived from UN WPP 2024 locations metadata (Aggregation_Lists sheet).",
  "All columns from the sheet are retained.",
  "Only minimal renaming is performed: LocPrintName->location and ISO3Code->iso3.",
  "Use this key to map UN location identifiers/codes to printable names and ISO3 codes."
)

notes_path <- file.path(OUT_DIR, "DATASET_NOTES.md")
notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("- ", dataset_notes)
)
writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------------------------
# Dataset notes (kept with cleaned data) - isoRegions
# ------------------------------------------------------------------------------
dataset_notes_iso <- c(
  "This dataset is a simplified ISO3-to-region lookup derived from UN WPP 2024 locations metadata (Aggregation_Lists sheet).",
  "Rows are restricted to ParentTypeName in {Region, Subregion, Income Group}.",
  "Rows are excluded when ParentPrintName is one of: High-and-upper-middle-income countries; Low-and-Lower-middle-income countries; Low-and-middle-income countries; Middle-income countries.",
  "Data are reshaped wide so there is one row per iso3 with columns: region, subregion, incomegr."
)

notes_path_iso <- file.path(OUT_DIR_ISO, "DATASET_NOTES.md")
notes_lines_iso <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste0("- ", dataset_notes_iso)
)
writeLines(notes_lines_iso, notes_path_iso)
message("Wrote dataset notes: ", notes_path_iso)

# ------------------------------------------------------------------------------
# Variable dictionary (lightweight) - location_key
# ------------------------------------------------------------------------------
var_dict <- data.frame(
  var = c("location", "iso3"),
  var_label = c(
    "UN WPP printable location name",
    "ISO3 code (as provided in WPP metadata)"
  ),
  notes = c(
    "Renamed from LocPrintName.",
    "Renamed from ISO3Code; may be blank for non-country aggregates."
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(OUT_DIR, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

doc_warnings <- doc_warnings_for_var_dict(
  df_clean,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "df_clean"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}

# ------------------------------------------------------------------------------
# Variable dictionary - isoRegions
# ------------------------------------------------------------------------------
var_dict_iso <- data.frame(
  var = c("iso3", "location", "region", "subregion", "incomegr"),
  var_label = c(
    "ISO3 code (as provided in WPP metadata)",
    "UN WPP printable location name",
    "UN WPP region name",
    "UN WPP subregion name",
    "UN WPP income group name"
  ),
  notes = c(
    "This file includes only non-missing ISO3.",
    "Renamed from LocPrintName.",
    "Derived from ParentTypeName == 'Region' (ParentPrintName).",
    "Derived from ParentTypeName == 'Subregion' (ParentPrintName).",
    "Derived from ParentTypeName == 'Income Group' (ParentPrintName), with select aggregates removed."
  ),
  stringsAsFactors = FALSE
)

vars_path_iso <- file.path(OUT_DIR_ISO, "variables_info.csv")
write.csv(var_dict_iso, vars_path_iso, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path_iso)

