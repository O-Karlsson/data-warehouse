# R/build_wbi_neonatal_mortality.R
#
# PURPOSE
# -------
# Download neonatal mortality (UN IGME via World Bank WDI),
# store raw data, and create a minimal cleaned dataset.
#
# Indicator:
#   SH.DYN.NMRT  = Neonatal mortality rate (per 1,000 live births)
#
# Raw output:
#   data/raw/WBI/wdi_neonatal_mortality_raw.csv
#
# Cleaned output:
#   data/cleaned/WBI/data.csv
#   data/cleaned/WBI/data.parquet
#   data/cleaned/WBI/data.dta
#
# Cleaning rules:
#   - keep: country, iso3c, year, nmr
#   - rename: iso3c -> iso3
#   - no further cleaning

# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

source("R/utils.R")

suppressPackageStartupMessages({
  library(WDI)
  library(dplyr)
  library(readr)
  library(arrow)
  library(haven)
})

# ------------------------------------------------------------------------------
# Directories
# ------------------------------------------------------------------------------

dir_raw   <- file.path("data", "raw", "WBI", "neonatal mortality")
dir_clean <- file.path("data", "cleaned", "WBI", "neonatal mortality")

dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 1) Download raw data
# ------------------------------------------------------------------------------

indicator <- c(nmr = "SH.DYN.NMRT")

raw_df <- WDI(
  country   = "all",
  indicator = indicator,
  start     = 1960,
  end       = as.integer(format(Sys.Date(), "%Y")),
  extra     = TRUE,
  cache     = NULL
)

raw_path <- file.path(dir_raw, "wdi_neonatal_mortality_raw.csv")
write_csv(raw_df, raw_path, na = "")
message("Wrote raw data: ", raw_path)

# ------------------------------------------------------------------------------
# 2) Clean (minimal, warehouse-style)
# ------------------------------------------------------------------------------

clean_df <- raw_df %>%
  transmute(
    location = country,
    region  = region,
    iso3    = iso3c,
    year    = year,
    nmr     = nmr
  )
clean_df <- clean_df %>%
  arrange(location, year)
# ------------------------------------------------------------------------------
# 3) Write outputs (parquet + dta + csv)
# ------------------------------------------------------------------------------

# *****************************************************************************
# OLD simple write (no archiving): uncomment to use (and remove code blocks below)
# arrow::write_parquet(clean_df, file.path(dir_clean, "data.parquet"))
# haven::write_dta(clean_df,     file.path(dir_clean, "data.dta"))
# readr::write_csv(clean_df,     file.path(dir_clean, "data.csv"))
# *****************************************************************************

# *****************************************************************************
# Build script output + archiving pattern

# Note: only checks if parquet changed, since dta may have  
#       metadata differences even if data is identical.
#       Makes use of utility function archive_and_promote() from R/utils.R

# Archive folder (for old versions, if files have changed)
arch_dir <- file.path(dir_clean, "_archive")

# Exisiting data file (for comparison)
out_parquet_final <- file.path(dir_clean, paste0("data", ".parquet"))

# 1) Write the NEW parquet to a temp file first (never overwrite directly)
out_parquet_tmp <- tempfile(fileext = ".parquet")
arrow::write_parquet(clean_df, out_parquet_tmp)

# 2) Archive-and-promote:
#    - If parquet exists and differs from the new one, copy old to archive
#    - Then move tmp -> final
res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

# 3) If parquet changed (res.changed==TRUE) archive old .dta and .csv too
if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  file.copy(file.path(dir_clean, "data.dta"),
            file.path(arch_dir, paste0("data_", tag, ".dta")),
            overwrite = FALSE)
  
  file.copy(file.path(dir_clean, "data.csv"),
            file.path(arch_dir, paste0("data_", tag, ".csv")),
            overwrite = FALSE)
}

# then overwrite exports
haven::write_dta(clean_df, file.path(dir_clean, "data.dta"))
readr::write_csv(clean_df, file.path(dir_clean, "data.csv"))

# ------------------------------------------------------------------------------
# 4) SOURCES.md (raw)
# ------------------------------------------------------------------------------

sources_md <- file.path(dir_raw, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Source: World Bank World Development Indicators (WDI)",
      "Indicator: SH.DYN.NMRT – Neonatal mortality rate (per 1,000 live births)",
      "Estimates produced by the UN Inter-agency Group for Child Mortality Estimation (UN IGME).",
      "Upper and lower bounds for estimates are available in original source at: https://data.unicef.org/topic/child-survival/neonatal-mortality/#data",
      "Not available disaggregated by sex (consider IHME for that)",
      paste0("Accessed on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# ------------------------------------------------------------------------------
# 5) Dataset notes (cleaned)
# ------------------------------------------------------------------------------

notes_path <- file.path(dir_clean, "DATASET_NOTES.md")

notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  "- Neonatal mortality rates are expressed per 1,000 live births.",
  "- Estimates are produced by UN IGME and distributed via the World Bank WDI.",
  "- Country coverage and year availability follow WDI conventions.",
  "- Not available disaggregated by sex (consider IHME for that)"
)

writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------------------------
# 6) Variable dictionary
# ------------------------------------------------------------------------------

var_dict <- data.frame(
  var = c("location", "iso3", "region", "year", "nmr"),
  var_label = c(
    "Location name",
    "ISO 3166-1 alpha-3 country code",
    "Region",
    "Calendar year",
    "Neonatal mortality rate (per 1,000 live births)"
  ),
  notes = c(
    "Country, region, income group, etc.",
    "",
    "",
    "",
    "Deaths during the first 28 days of life per 1,000 live births"
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(dir_clean, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------------------------
# Documentation check
# ------------------------------------------------------------------------------

doc_warnings <- doc_warnings_for_var_dict(
  clean_df,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "clean_df"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}
message("Done.")
