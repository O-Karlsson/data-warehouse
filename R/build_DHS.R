# R/build_DHS.R
#
# Purpose
# -------
# Bulk-download DHS datasets (via the DHS API through the `rdhs` package) by:
#   1) selecting datasets by FileType and SurveyYear
#   2) applying file-format rules (Stata for most; ASCII for GPS-like)
#   3) downloading ZIP archives to data/raw/DHS/<FileType>/
#   4) unzipping contents to data/cleaned/DHS/<FileType>/
#   5) writing metadata snapshots:
#        - data/raw/DHS/metadata_raw.csv     (all available datasets)
#        - data/cleaned/DHS/metadata.csv     (datasets unzipped by this pipeline, cumulative)
#
# Note, we are using rdhs in an unconventional way and therefore need
# remove cache subfolders to force fresh downloads each time and move files across folders
# ourselves. Credentials need to be set up once per user/session as described below.
#
# Output structure
# ----------------
#   data/raw/DHS/metadata_raw.csv
#   data/raw/DHS/<FileType>/<FileName>.zip
#   data/cleaned/DHS/<FileType>/...          (unzipped files)
#   data/cleaned/DHS/metadata.csv
#
# Notes on credentials
# --------------------
# This script intentionally does NOT contain any personal credentials.
# Each user must run rdhs::set_rdhs_config(...) ONCE in an interactive session.
# An account and a project at DHSPROGRAM.COM are required; see rdhs documentation for details.
#
# Notes on cache clearing
# -----------------------
# rdhs caches API key lookups and downloaded payloads. For this pipeline we want
# a "fresh" run each time, so we selectively delete subfolders of the rdhs cache:
#   - db/keys : cached request keys / lookup mapping
#   - data    : cached downloaded content
# We do NOT delete the cache root or db/ entirely because rdhs may keep internal
# structures there depending on version.

library(rdhs)

# -------------------------
# 1) Ensure rdhs credentials/config are available
# -------------------------
# get_rdhs_config() returns the rdhs configuration ACTIVE IN THIS SESSION.
# If it is NULL, rdhs does not know your credentials and cannot call the DHS API.
cfg <- get_rdhs_config()
if (is.null(cfg)) {
  stop(
    "DHS credentials not configured.\n\n",
    "Please run once in an interactive R session:\n\n",
    "  rdhs::set_rdhs_config(\n",
    "    email       = \"YOUR_EMAIL\",\n",
    "    project     = \"YOUR_PROJECT_NAME\",\n",
    "    config_path = \"~/.rdhs.json\",\n",
    "    global      = TRUE\n",
    "  )\n\n",
    "This writes credentials to ~/.rdhs.json and only needs to be done once per session."
  )
}

# -------------------------
# 2) Locate rdhs cache root and validate it
# -------------------------
# cfg$cache_path is where rdhs stores cached API lookups and downloaded files.
# We validate it before manipulating cache directories to avoid deleting the wrong
# folder (or trying to delete a NULL/invalid path).
cache_root <- cfg$cache_path

stopifnot(
  is.character(cache_root),   # must be a character path
  nzchar(cache_root),         # non-empty string
  dir.exists(cache_root)      # must exist on disk
)

# -------------------------
# 3) Selectively clear cache subfolders we care about
# -------------------------
# Why delete these?
#   - db/keys : forces fresh lookup of request keys/mappings
#   - data    : forces fresh downloads rather than reusing cached content
#
# We avoid deleting the cache root itself. Instead, we delete only these subfolders
# and then recreate them to ensure rdhs has somewhere to write.
paths_to_clear <- c(
  file.path(cache_root, "db", "keys"),
  file.path(cache_root, "db", "data")
)

for (p in paths_to_clear) {
  if (dir.exists(p)) {
    message("Clearing rdhs cache subdir: ", p)
    unlink(p, recursive = TRUE, force = TRUE)
  }
  # Ensure the directory exists after deletion (idempotent)
  dir.create(p, recursive = TRUE, showWarnings = FALSE)
}

# This is defensive: ensures the cache root exists even if a user points rdhs at a
# project-local cache path. Usually redundant because we asserted dir.exists().
dir.create(cache_root, recursive = TRUE, showWarnings = FALSE)

# -------------------------
# USER CONFIG (what to download)
# -------------------------
# FILETYPES_WANTED:
#   DHS FileType strings as returned by get_available_datasets().
#   Example: "Individual Recode" (IR), "Household Recode" (HR), etc.
FILETYPES_WANTED <- c(
  "Wealth Index",
  "Individual Recode",
  "Household Recode",
  "Geographic Data",
  "Births Recode"
)

# SURVEY_YEAR_RANGE:
#   Inclusive year window. We filter SurveyYear in [min, max].
SURVEY_YEAR_RANGE <- c(1980, 2026)

# FORCE_REDONWLOAD:
#   If TRUE, download ZIP again even if already present in data/raw.
FORCE_REDONWLOAD <- FALSE

# Folder roots for raw ZIPs and cleaned (unzipped) outputs.
RAW_ROOT   <- file.path("data", "raw", "DHS")
CLEAN_ROOT <- file.path("data", "cleaned", "DHS")

# -------------------------
# Helper functions
# -------------------------

# Create a directory if it doesn't exist; no warnings if it does.
dir_ok <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

# Convert a FileType string into a filesystem-safe folder name:
#   - trims whitespace
#   - replaces disallowed characters with underscores
#   - collapses multiple spaces
safe_name <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub("[/\\\\:*?\"<>|]+", "_", x)
  x <- gsub("\\s+", " ", x)
  x
}

# Download a single DHS dataset (by FileName) as a ZIP, returning the path to the
# ZIP file produced by rdhs.
#
# - We download to a temporary export directory under tempdir()
#   and then move the ZIP into our project folder to circumvent rdhs behavior.
download_zip <- function(file_name) {
  # Use a dedicated temp directory for rdhs export.
  export_dir <- file.path(tempdir(), "rdhs_export")
  
  # Remove any old export contents to prevent picking up stale ZIPs.
  unlink(export_dir, recursive = TRUE, force = TRUE)
  dir_ok(export_dir)
  
  # Ask rdhs to download ZIP into export_dir.
  # clear_cache = FALSE here because we manage cache above by deleting subfolders.
  get_datasets(
    dataset_filenames = file_name,
    download_option   = "zip",
    output_dir_root   = export_dir,
    clear_cache       = FALSE
  )
  
  # Find the ZIP rdhs created (location varies by rdhs internals/version).
  zips <- list.files(
    export_dir,
    recursive   = TRUE,
    full.names  = TRUE,
    pattern     = "\\.zip$",
    ignore.case = TRUE
  )
  
  if (length(zips) < 1) stop("No ZIP produced for: ", file_name)
  
  # Return the first ZIP found (rdhs should produce exactly one here).
  zips[1]
}

# -------------------------
# Main pipeline
# -------------------------

# Ensure output roots exist.
dir_ok(RAW_ROOT)
dir_ok(CLEAN_ROOT)

# 1) Pull the full list of DHS datasets available to your account/project.
message("Fetching available DHS datasets...")
ad <- get_available_datasets()

# Save the full "unfiltered" metadata snapshot for auditing/debugging.
write.csv(ad, file.path(RAW_ROOT, "metadata_raw.csv"), row.names = FALSE)

# 2) Normalize key columns used for filtering.
#    (rdhs returns these as factors/strings depending on environment.)
ad$FileType   <- trimws(as.character(ad$FileType))
ad$FileFormat <- trimws(as.character(ad$FileFormat))
ad$DatasetType <- trimws(as.character(ad$DatasetType))
ad$SurveyYear <- suppressWarnings(as.integer(ad$SurveyYear))

# 3) Filter by survey year window and desired FileTypes.
year_min <- min(SURVEY_YEAR_RANGE)
year_max <- max(SURVEY_YEAR_RANGE)

keep <- !is.na(ad$SurveyYear) &
  ad$SurveyYear >= year_min &
  ad$SurveyYear <= year_max

ad2 <- ad[keep, , drop = FALSE] # Filter rows

# Filter by filetype (if requested)
if (!is.null(FILETYPES_WANTED) && length(FILETYPES_WANTED) > 0) {
  ad2 <- ad2[ad2$FileType %in% trimws(FILETYPES_WANTED), , drop = FALSE]
}

# 4) Apply file-format rules.
#    - For most datasets: prefer Stata (.dta) ZIPs
#    - For GPS-like datasets: use Flat ASCII (.dat)
stata_fmt <- "Stata dataset (.dta)"
ascii_fmt <- "Flat ASCII data (.dat)"

# Identify GPS-like datasets either by FileType or by DatasetType when available.
gps_like <- (ad2$FileType %in% c("Geospatial Covariates", "Geographic Data")) |
  ("DatasetType" %in% names(ad2) & ad2$DatasetType == "GPS Datasets")

# Keep only rows that match the format rule.
ad2 <- ad2[
  (gps_like  & ad2$FileFormat == ascii_fmt) |
    (!gps_like & ad2$FileFormat == stata_fmt),
  , drop = FALSE
]

# If no datasets match, exit early.
if (nrow(ad2) == 0) {
  message("Nothing matched (FileTypes + SurveyYear + format rules).")
  quit(save = "no")
}

# 5) De-duplicate selection (some catalogs can contain duplicates).
if (all(c("SurveyId", "FileName") %in% names(ad2))) {
  ad2 <- ad2[!duplicated(paste(ad2$SurveyId, ad2$FileName, sep = "||")), , drop = FALSE]
}

message("Datasets selected: ", nrow(ad2))

# 6) Prepare cleaned metadata tracking.
#    metadata.csv will accumulate rows across runs so you can see what was unzipped.
clean_meta_path <- file.path(CLEAN_ROOT, "metadata.csv")
clean_meta_old <- if (file.exists(clean_meta_path)) {
  read.csv(clean_meta_path, stringsAsFactors = FALSE)
} else {
  NULL
}

unzipped_rows <- list() # empty list to collect datasets in metadata in cleaned

# 7) Download + unzip each dataset.
for (i in seq_len(nrow(ad2))) {
  r <- ad2[i, , drop = FALSE]
  
  file_name <- as.character(r$FileName)
  file_type <- safe_name(r$FileType)  # folder-safe FileType name
  
  # Construct per-FileType folders.
  raw_dir     <- file.path(RAW_ROOT, file_type)
  cleaned_dir <- file.path(CLEAN_ROOT, file_type)
  dir_ok(raw_dir)
  dir_ok(cleaned_dir)
  
  # Final destination of the ZIP in the raw folder.
  dest_zip <- file.path(raw_dir, file_name)
  
  # ---- Download step ----
  # If FORCE_REDONWLOAD is TRUE OR the ZIP doesn't already exist, download.
  if (FORCE_REDONWLOAD || !file.exists(dest_zip)) {
    message(sprintf("[%d/%d] Downloading %s", i, nrow(ad2), file_name))
    
    # Download ZIP to temp export dir and then move/copy into raw folder.
    tmp_zip <- download_zip(file_name)
    
    # Prefer rename() (fast move) but fall back to copy+delete if needed (e.g. across drives).
    ok <- file.rename(tmp_zip, dest_zip)
    if (!ok) {
      file.copy(tmp_zip, dest_zip, overwrite = TRUE)
      unlink(tmp_zip, force = TRUE)
    }
  } else {
    message(sprintf("[%d/%d] ZIP exists, skip: %s", i, nrow(ad2), file_name))
  }
  
  # ---- Unzip step ----
  # Unzip into cleaned folder. overwrite=TRUE ensures re-runs are deterministic.
  message(sprintf("[%d/%d] Unzipping -> %s", i, nrow(ad2), cleaned_dir))
  unzip(dest_zip, exdir = cleaned_dir, overwrite = TRUE)
  
  # Track that this row was processed/unzipped in this run.
  unzipped_rows[[length(unzipped_rows) + 1]] <- r
}

# 8) Update cumulative cleaned metadata.csv with rows processed this run.
if (length(unzipped_rows) > 0) {
  new_meta <- do.call(rbind, unzipped_rows)
  
  combined <- if (!is.null(clean_meta_old) && nrow(clean_meta_old) > 0) {
    rbind(clean_meta_old, new_meta)
  } else {
    new_meta
  }
  
  # De-duplicate again to keep the metadata file tidy across multiple runs.
  if (all(c("SurveyId", "FileName") %in% names(combined))) {
    combined <- combined[
      !duplicated(paste(combined$SurveyId, combined$FileName, sep = "||")),
      ,
      drop = FALSE
    ]
  }
  
  write.csv(combined, clean_meta_path, row.names = FALSE)
  message("Updated: ", clean_meta_path)
} else {
  message("No new unzips; metadata.csv unchanged.")
}


# ------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------

# ------------------------------------------------------------------------------
# Write SOURCES.md to keep  with raw data
# ------------------------------------------------------------------------------

sources_md <- file.path(RAW_ROOT, "SOURCES.md")
if (!file.exists(sources_md)) {
  writeLines(
    c(
      "Access point: https://dhsprogram.com/",
      "Requies and account and an application for a project.",
      "Downloaded from API using rdhs package.",
      "metadata_raw.csv shows all datasets available under the project (see credentials).",
      paste0("This file (SOURCES.md) was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  message("Wrote sources file: ", sources_md)
}

# ------------------------------------------------------------------------------
# Write info to keep with cleaned data
# ------------------------------------------------------------------------------

dataset_notes <- c(
  "No cleaning: only unzipped from downloaded files in RAW.",
  "List of surveys, survey IDs, years, country names and other useful information stored in metadata.csv",
  "See codebooks and other info: https://dhsprogram.com/publications/publication-dhsg4-dhs-questionnaires-and-manuals.cfm."
)
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



message("Done.")