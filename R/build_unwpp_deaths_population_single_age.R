# R/build_unwpp_deaths_population_single_age.R
#
# PURPOSE
# -------
# Download, clean, join, and reshape UN WPP 2024 population and deaths data
# (single age, sex, year), keeping estimates and projections separate.
#
# Files (4 total):
#  1) Population by single age/sex, estimates   (1950–2023)
#  2) Deaths by single age/sex, estimates       (1950–2023)
#  3) Population by single age/sex, projections (2024–2100)
#  4) Deaths by single age/sex, projections     (2024–2100)
#
# Join keys:
#   LocID, Time, AgeGrpStart
#
# Reshape:
#   PopMale/PopFemale/PopTotal   -> pop   with sex=1/2/3
#   DeathMale/DeathFemale/DeathTotal -> deaths with sex=1/2/3
#
# Keep variables:
#   LocID, Location, Time, AgeGrpStart, ISO3_code, deaths, pop, sex
#
# Rename:
#   Location    -> location
#   Time        -> year
#   AgeGrpStart -> age
#   ISO3_code   -> iso3
#
# Raw storage:
#   data/raw/unwpp/population and deaths/<estimates|projections>/
#
# Cleaned storage:
#   data/cleaned/unwpp/population and deaths/<estimates|projections>/
#
# Outputs:
#   - data.parquet (primary, efficient)
#   - data.dta
#   - data.csv.gz
#
# Sources (UN WPP 2024):
#   Population estimates:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz
#   Population projections:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz
#   Deaths estimates:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_DeathsBySingleAgeSex_Medium_1950-2023.csv.gz
#   Deaths projections:
#     https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_DeathsBySingleAgeSex_Medium_2024-2100.csv.gz

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------
source("R/utils.R")

suppressPackageStartupMessages({
  library(vroom)
  library(dplyr)
  library(tidyr)
  library(arrow)
  library(haven)
  library(readr)
})

URLS <- list(
  estimates = list(
    pop   = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_1950-2023.csv.gz",
    deaths = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_DeathsBySingleAgeSex_Medium_1950-2023.csv.gz"
  ),
  projections = list(
    pop   = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_PopulationBySingleAgeSex_Medium_2024-2100.csv.gz",
    deaths = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_DeathsBySingleAgeSex_Medium_2024-2100.csv.gz"
  )
)

KEEP_POP <- c("LocID", "Time", "AgeGrpStart", "ISO3_code", "PopMale", "PopFemale", "PopTotal")
KEEP_DEA <- c("LocID", "Time", "AgeGrpStart", "DeathMale", "DeathFemale", "DeathTotal")

# Explicit types for speed + stability
COL_TYPES_POP <- vroom::cols(
  LocID       = vroom::col_integer(),
  Time        = vroom::col_integer(),
  AgeGrpStart = vroom::col_integer(),
  ISO3_code   = vroom::col_character(),
  PopMale     = vroom::col_double(),
  PopFemale   = vroom::col_double(),
  PopTotal    = vroom::col_double(),
  .default    = vroom::col_skip()
)

COL_TYPES_DEA <- vroom::cols(
  LocID       = vroom::col_integer(),
  Time        = vroom::col_integer(),
  AgeGrpStart = vroom::col_integer(),
  DeathMale   = vroom::col_double(),
  DeathFemale = vroom::col_double(),
  DeathTotal  = vroom::col_double(),
  .default    = vroom::col_skip()
)

base_raw   <- file.path("data", "raw", "unwpp", "population and deaths")
base_clean <- file.path("data", "cleaned", "unwpp", "population and deaths")
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

read_pop <- function(path) {
  vroom::vroom(
    file       = path,
    delim      = ",",
    col_select = any_of(KEEP_POP),
    col_types  = COL_TYPES_POP,
    progress   = FALSE,
    altrep     = TRUE
  )
}

read_deaths <- function(path) {
  vroom::vroom(
    file       = path,
    delim      = ",",
    col_select = any_of(KEEP_DEA),
    col_types  = COL_TYPES_DEA,
    progress   = FALSE,
    altrep     = TRUE
  )
}

# ------------------------------------------------------------------------------
# Build (estimates + projections)
# ------------------------------------------------------------------------------
for (period in names(URLS)) {
  
  dir_raw   <- file.path(base_raw, period)
  dir_clean <- file.path(base_clean, period)
  dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)
  
  u_pop <- URLS[[period]]$pop
  u_dea <- URLS[[period]]$deaths
  
  f_pop <- file.path(dir_raw, basename(utils::URLdecode(u_pop)))
  f_dea <- file.path(dir_raw, basename(utils::URLdecode(u_dea)))
  
  # 1) Download raw
  download_if_needed(u_pop, f_pop)
  download_if_needed(u_dea, f_dea)
  
  # 2) Read (select only needed cols)
  pop <- read_pop(f_pop)
  dea <- read_deaths(f_dea)
  
  # 3) Join + reshape (fast + single pivot)
  df_clean <- pop %>%
    left_join(dea, by = c("LocID", "Time", "AgeGrpStart")) %>%
    # normalize names for a single pivot_longer with .value
    rename(
      pop_Male    = PopMale,
      pop_Female  = PopFemale,
      pop_Total   = PopTotal,
      deaths_Male   = DeathMale,
      deaths_Female = DeathFemale,
      deaths_Total  = DeathTotal
    ) %>%
    pivot_longer(
      cols      = c(pop_Male, pop_Female, pop_Total, deaths_Male, deaths_Female, deaths_Total),
      names_to  = c(".value", "sex_name"),
      names_sep = "_"
    ) %>%
    mutate(
      sex = dplyr::case_when(
        sex_name == "Male"   ~ 1L,
        sex_name == "Female" ~ 2L,
        sex_name == "Total"  ~ 3L,
        TRUE ~ NA_integer_
      )
    ) %>%
    select(
      LocID,
      Time,
      AgeGrpStart,
      ISO3_code,
      deaths,
      pop,
      sex
    ) %>%
    rename(
      year     = Time,
      age      = AgeGrpStart,
      iso3     = ISO3_code
    ) %>%
    # keep column order stable + efficient
    arrange(LocID, year, age, sex)
  
  # 4) Write outputs (archive + promote pattern)
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
      "Dataset: World Population Prospects 2024 (WPP 2024) population and deaths by single age and sex (Medium).",
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
  "This dataset combines UN WPP 2024 population counts and deaths by location, year, and single-year age.",
  "Within each period (estimates vs projections), a population file and a deaths file are downloaded and joined on LocID, Time, and AgeGrpStart.",
  "The joined data are reshaped from wide sex-specific columns to long format.",
  "Sex-specific columns PopMale/PopFemale/PopTotal and DeathMale/DeathFemale/DeathTotal are converted to rows with variables pop and deaths and an integer sex code (1=Male, 2=Female, 3=Total).",
  "Minimal harmonization is performed: ISO3_code->iso3, Time->year, AgeGrpStart->age.",
  "Estimates and projections are stored separately and are not combined in the cleaned outputs.",
  "Units/definitions (e.g., mid-year population vs period exposure; deaths definition) follow UN WPP documentation and may differ across WPP vintages."
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
  var = c("LocID", "year", "age", "iso3", "sex", "pop", "deaths"),
  var_label = c(
    "UN WPP location identifier",
    "Calendar year",
    "Age (years), single-year age group start",
    "ISO3 country code",
    "Sex identifier",
    "Population count",
    "Number of deaths"
  ),
  notes = c(
    "",
    "",
    "Single-year age; interpreted as start of age interval.",
    "",
    "1=Male, 2=Female, 3=Total (created by reshaping wide columns).",
    "Derived from PopMale/PopFemale/PopTotal after reshaping. See UN WPP documentation for population definition.",
    "Derived from DeathMale/DeathFemale/DeathTotal after reshaping. See UN WPP documentation for deaths definition."
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
