# R/build_hmd_france_subregions1_life_tables.R
#
# PURPOSE
# -------
# Download, clean, and combine French subregional (NUTS1) 1x1 period life tables
# into a single dataset.
#
# Differences vs Japan script:
#   - Source/URL pattern: frdata.org FHMD instead of IPSS JMD
#   - Regions: numeric 1..13 (NUTS1) instead of "00".."47"
#   - Sex codes: "M","F","T" (Total) instead of "m","f","b"
#   - File format: ';' separated with decimal comma (dec=",") and header on first row
#     (no title line / blank line to skip)
#   - Region names: provided mapping (not parsed from file header)

# ------------------------------------------------------------------------------
# Global constants
# ------------------------------------------------------------------------------
source("R/utils.R")

BASE_URL <- "https://frdata.org/data/fhmd/1x1-NUTS1"
regions  <- 1:13
sexes    <- c("M", "F", "T")   # Male, Female, Total (both)

# ------------------------------------------------------------------------------
# Directory setup
# ------------------------------------------------------------------------------
dir_raw <- file.path("data","raw","HMD","life tables","France")
dir_clean <- file.path("data","cleaned","HMD","life tables","France")

dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# Region names (France-specific: supplied mapping, not parsed from files)
# ------------------------------------------------------------------------------
region_lookup <- data.frame(
  region_id = 1:13,
  name = c(
    "Île de France",
    "Centre-Val de Loire",
    "Bourgogne-Franche-Comté",
    "Normandie",
    "Hauts-de-France",
    "Grand Est",
    "Pays de la Loire",
    "Bretagne",
    "Nouvelle-Aquitaine",
    "Occitanie",
    "Auvergne-Rhône-Alpes",
    "Provence-Alpes-Côte d’Azur",
    "Corse"
  ),
  stringsAsFactors = FALSE
)

# ------------------------------------------------------------------------------
# 1) Download raw files (only if missing or empty)
# ------------------------------------------------------------------------------
for (r in regions) for (s in sexes) {
  
  # France-specific URL: .../1x1-NUTS1/[region][sex].txt
  url  <- sprintf("%s/%d%s.txt", BASE_URL, r, s)
  
  # Local filename mirrors the Japan style: stable, explicit, includes region+sex
  dest <- file.path(dir_raw, sprintf("fr_nuts1_%02d_%s.txt", r, s))
  
  if (file.exists(dest) && file.info(dest)$size > 0) next
  
  ok <- tryCatch({
    suppressWarnings(
      utils::download.file(url, dest, mode = "wb", quiet = TRUE)
    )
    file.exists(dest) && file.info(dest)$size > 0
  }, error = function(e) FALSE)
  
  if (!ok) {
    if (file.exists(dest)) unlink(dest)
    stop(sprintf("Download failed: %s -> %s", url, dest), call. = FALSE)
  }
}

# ------------------------------------------------------------------------------
# 2) Read and append all files
# ------------------------------------------------------------------------------
index <- expand.grid(region_id = regions, sex = sexes, stringsAsFactors = FALSE)
index$path <- mapply(
  function(r, s) file.path(dir_raw, sprintf("fr_nuts1_%02d_%s.txt", r, s)),
  index$region_id,
  index$sex
)

if (any(!file.exists(index$path))) {
  stop(
    "Some expected raw files are missing:\n",
    paste(index$path[!file.exists(index$path)], collapse = "\n"),
    call. = FALSE
  )
}

clean_df <- do.call(rbind, Map(function(path, r, s) {
  
  # France-specific read:
  # - ';' separated
  # - decimal comma in numeric fields (dec=",")
  # - header is on the first row (no skip)
  df <- utils::read.delim(
    path,
    sep = ";",
    dec = ",",
    header = TRUE,
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  # Clean column names (same logic as Japan)
  nm <- tolower(names(df))
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  names(df) <- make.unique(nm, sep = "_")
  
  # Normalize age if present (same logic as Japan)
  if ("age" %in% names(df)) {
    df$age <- as.integer(gsub("\\+", "", df$age))
  }
  
  # Identifiers (France-specific: region_id integer + name from lookup)
  df$region_id <- as.integer(r)
  df$sex       <- c(M = 1L, F = 2L, T = 3L)[s]
  df$name      <- region_lookup$name[match(df$region_id, region_lookup$region_id)]
  
  df
}, index$path, index$region_id, index$sex))

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
# *****************************************************************************

# ------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------
# These notes describe scope, sources, and caveats for the
# dataset as a whole (not individual variables).
# Edit the content below as needed.
# ------------------------------------------------------------


# ------------------------------------------------------------------------------
# Write SOURCES.md to keep  with raw data (only if at least one file was downloaded)
# ------------------------------------------------------------------------------

sources_md <- file.path(dir_raw, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Access point: https://frdata.org/fr/french-human-mortality-database/",
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
  "This dataset contains 1x1 period life tables for Canada and its subregions.",
  "Source data originate from the Human Mortality Database (HMD).",
  "Life tables are harmonized to single-year age intervals.",
  "Variable names (mostly) follow standard life table notations (although in lowercase)."
)
notes_path <- file.path(dir_clean, "DATASET_NOTES.md")


notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste("- ", dataset_notes)
)

# Write notes file
writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------
# ------------------------------------------------------------
# ------------------------------------------------------------
# Variable dictionary (edit var_label / notes here)
# ------------------------------------------------------------
# ------------------------------------------------------------

cat(paste(names(clean_df), collapse = "\n"), "\n")

var_dict <- data.frame(
  var = c("year", "sex", "age", "lx","lx_1","region_id","name"),
  var_label = c(
    "Calendar year",
    "Sex",
    "Age in years",
    "lx (number of survivors at age x)",
    "Lx, number of person-years lived between ages x and x+1",
    "ID for region",
    "Name of region"
  ),
  notes = c(
    "",
    "1=male, 2=female",
    "Single-year age. 110+ stored as 110",
    "",
    "Names are all lowercase, therefore _1 was added to Lx","ID is made up (not from raw file",""
  ),
  stringsAsFactors = FALSE
)

# ------------------------------------------------------------
# Write variable dictionary
# ------------------------------------------------------------

vars_path <- file.path(dir_clean, "variables_info.csv")

write.csv(
  var_dict,
  vars_path,
  row.names = FALSE,
  na = ""
)

message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------
# Emit documentation warnings at the very end
# ------------------------------------------------------------
# Collect documentation warnings (function from R/utils.R)

doc_warnings <-doc_warnings_for_var_dict(clean_df, 
  var_dict, dict_name = "variables_info.csv", df_name = "clean_df")

if (length(doc_warnings) > 0) {
  warning(
    paste(doc_warnings, collapse = "\n"),
    call. = FALSE
  )
}

message("Done.")
