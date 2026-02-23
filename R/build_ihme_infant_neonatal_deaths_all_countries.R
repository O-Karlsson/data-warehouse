# R/build_ihme_infant_neonatal_deaths_all_countries.R
#
# Cleans IHME GBD Results export where two outcomes are stored in separate rows:
#   - age_name == "<1 year"   -> dth1
#   - age_name == "<28 days"  -> dthn
# Values are in column: val
#
# Output is reshaped WIDE so each (location_id, sex, year) has columns dth1 and dthn.

source("R/utils.R")

suppressPackageStartupMessages({
  library(readr)
  library(arrow)
  library(haven)
  library(dplyr)
  library(tidyr)
})

# ---- Paths (edit ZIP_PATH if needed) ----
ZIP_PATH <- "data/raw/GBD/Number of deaths all causes before age 28 days and before age 1 year for all countries/IHME-GBD_2023_DATA-b743d483-1.zip"
OUT_DIR <- file.path(
  "data", "cleaned", "GBD",
  "Number of deaths all causes before age 28 days and before age 1 year for all countries"
)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

if (!(file.exists(ZIP_PATH) && file.info(ZIP_PATH)$size > 0)) {
  stop(
    "Missing raw ZIP:\n  ", ZIP_PATH, "\n\n",
    "Place the manually downloaded IHME ZIP at this path.",
    call. = FALSE
  )
}

# ---- Find the CSV inside the ZIP ----
zlist <- utils::unzip(ZIP_PATH, list = TRUE)
csvs  <- zlist$Name[grepl("\\.csv$", zlist$Name, ignore.case = TRUE)]

if (length(csvs) != 1) {
  stop(
    "Expected exactly 1 CSV inside the ZIP, found ", length(csvs), ":\n  ",
    paste(csvs, collapse = "\n  "),
    call. = FALSE
  )
}

# ---- Read CSV directly from ZIP (no extraction needed) ----
con <- unz(ZIP_PATH, csvs[[1]], open = "rb")
on.exit(try(close(con), silent = TRUE), add = TRUE)

df <- readr::read_csv(con, show_col_types = FALSE)

# ---- Keep expected columns ----
keep <- c("location_id", "location_name", "sex_id", "age_name", "year", "val")
missing <- setdiff(keep, names(df))
if (length(missing) > 0) {
  stop("Missing expected columns in CSV:\n  ", paste(missing, collapse = "\n  "), call. = FALSE)
}

out_long <- df[, keep] %>%
  rename(sex = sex_id) %>%
  mutate(
    location_id = as.integer(location_id),
    sex         = as.integer(sex),
    year        = as.integer(year),
    outcome = dplyr::case_when(
      age_name == "<1 year"   ~ "dth1",
      age_name == "<28 days"  ~ "dthn",
      TRUE                    ~ NA_character_
    )
  ) %>%
  filter(!is.na(outcome)) %>%
  select(-age_name)

# ---- Guard against duplicates before widening ----
dups <- out_long %>%
  count(location_id, location_name, sex, year, outcome) %>%
  filter(n > 1)

if (nrow(dups) > 0) {
  stop(
    "Found duplicate rows for the same (location_id, sex, year, outcome). ",
    "Inspect the raw export; expected unique values per key.\n\n",
    paste0(capture.output(print(dups, n = 50)), collapse = "\n"),
    call. = FALSE
  )
}

# ---- Reshape wide: val -> dth1/dthn ----
out <- out_long %>%
  tidyr::pivot_wider(
    names_from  = outcome,
    values_from = val
  )

# ----------------------------------------------------------------------
# Write outputs (parquet + dta + csv) with archiving pattern
# ----------------------------------------------------------------------

arch_dir <- file.path(OUT_DIR, "_archive")
out_parquet_final <- file.path(OUT_DIR, "data.parquet")

# 1) Write the NEW parquet to a temp file first (never overwrite directly)
out_parquet_tmp <- tempfile(fileext = ".parquet")
arrow::write_parquet(out, out_parquet_tmp)

# 2) Archive-and-promote parquet (utility from R/utils.R)
res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

# 3) If parquet changed, archive old .dta and .csv too (if they exist)
if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  old_dta <- file.path(OUT_DIR, "data.dta")
  old_csv <- file.path(OUT_DIR, "data.csv")
  
  if (file.exists(old_dta)) {
    file.copy(old_dta, file.path(arch_dir, paste0("data_", tag, ".dta")), overwrite = FALSE)
  }
  if (file.exists(old_csv)) {
    file.copy(old_csv, file.path(arch_dir, paste0("data_", tag, ".csv")), overwrite = FALSE)
  }
}

# then overwrite exports
haven::write_dta(out, file.path(OUT_DIR, "data.dta"))
readr::write_csv(out, file.path(OUT_DIR, "data.csv"))

# ------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------

yr_min <- suppressWarnings(min(out$year, na.rm = TRUE))
yr_max <- suppressWarnings(max(out$year, na.rm = TRUE))
n_loc  <- length(unique(out$location_id))
n_yrs  <- length(unique(out$year))

dataset_notes <- c(
  "This dataset contains two all-cause number of deaths from IHME GBD results, reshaped to wide format:",
  "  - dth1: age_name == '<1 year' (number deaths before age 1 year)",
  "  - dthn: age_name == '<28 days' (number of deaths before age 28 days)",
  "Data were manually obtained from the Institute for Health Metrics and Evaluation (IHME) Global Burden of Disease (GBD) Results tool:",
  "https://vizhub.healthdata.org/gbd-results/ (requires login).",
  sprintf("Coverage: %s unique locations; %s unique years (%s–%s).", n_loc, n_yrs, yr_min, yr_max),
  "Sex coding follows IHME: 1=male, 2=female, 3=both.",
  "If either dth1 or dthn is missing for a row, the corresponding outcome was not present in the raw export for that key."
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

# ------------------------------------------------------------
# Variable dictionary (edit var_label / notes here)
# ------------------------------------------------------------

var_dict <- data.frame(
  var = c("location_id", "location_name", "sex", "year", "dth1", "dthn"),
  var_label = c(
    "IHME ID for location",
    "Name of location",
    "Sex",
    "Calendar year",
    "Number of deaths before age 1 year",
    "Number of deaths before age 28 days"
  ),
  notes = c(
    "See IHME Keys file for location codes (from the same export/download).",
    "",
    "1=male, 2=female, 3=both",
    "",
    "Derived from rows where age_name == '<1 year' (val).",
    "Derived from rows where age_name == '<28 days' (val)."
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(OUT_DIR, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------
# Emit documentation warnings at the very end
# ------------------------------------------------------------

doc_warnings <- doc_warnings_for_var_dict(
  out,
  var_dict,
  dict_name = "variables_info.csv",
  df_name   = "out"
)

if (length(doc_warnings) > 0) {
  warning(paste(doc_warnings, collapse = "\n"), call. = FALSE)
}

message("Done.")