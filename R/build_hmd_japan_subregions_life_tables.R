# R/build_hmd_japan_subregions_life_tables.R
#
# PURPOSE
# -------
# Download, clean, and combine Japanese subregional life tables
# from the IPSS Japanese Mortality Database (JMD) into a single dataset.
#
# The script:
#   1) Downloads raw life table text files (if missing)
#   2) Reads and appends all regions × sexes into one data frame
#   3) Cleans column names and fixes age formatting
#   4) Extracts region names from file headers
#   5) Writes the combined dataset to Parquet, Stata, and CSV formats
#
# The script is self-contained and does not rely on
# project-level config or utility helpers.

# ------------------------------------------------------------------------------
# Packages
# ------------------------------------------------------------------------------
# arrow  : writing Parquet files (compact, cross-language format)
# haven  : writing Stata .dta files
# readr  : writing CSV files with consistent encoding
library(arrow)
library(haven)
library(readr)
source("R/utils.R")

# ------------------------------------------------------------------------------
# Global constants
# ------------------------------------------------------------------------------
# Base URL for IPSS Japanese Mortality Database (JMD)
BASE_URL <- "https://www.ipss.go.jp/p-toukei/JMD"

# Region codes used by JMD:
#   "00" = Japan total
#   "01"–"47" = prefectures
regions  <- sprintf("%02d", 0:47)

# Sex codes used by JMD:
#   b = both sexes
#   m = male
#   f = female
sexes    <- c("b", "m", "f")

# ------------------------------------------------------------------------------
# Directory setup
# ------------------------------------------------------------------------------
# Raw data directory (downloaded text files)
dir_raw <- file.path("data","raw","HMD","life tables","Japan")
dir_clean <- file.path("data","cleaned","HMD","life tables","Japan")


# Ensure both directories exist (idempotent)
dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 1) Download raw files (only if missing or empty)
# ------------------------------------------------------------------------------
# Loop over all region × sex combinations
for (r in regions) for (s in sexes) {
  
  # Construct remote URL for the life table text file
  url <- sprintf("%s/%s/STATS/%sltper_1x1.txt", BASE_URL, r, s)
  
  # Local destination path for the downloaded file
  dest <- file.path(dir_raw, sprintf("jmd_%s_%sltper_1x1.txt", r, s))
  
  # Skip download if the file already exists and is non-empty
  if (file.exists(dest) && file.info(dest)$size > 0) next
  
  # Attempt download; suppress warnings and capture success/failure
  # ok will never be an error because of tryCatch
  ok <- tryCatch({
    suppressWarnings(
      utils::download.file(url, dest, mode = "wb", quiet = TRUE)
    )
    file.exists(dest) && file.info(dest)$size > 0 # was the download successful? Only the last expression before the comma matters for what is returned
  }, error = function(e) FALSE)
  
  # If download failed, remove any partial file and stop with a clear error
  if (!ok) {
    if (file.exists(dest)) unlink(dest)
    stop(sprintf("Download failed: %s -> %s", url, dest), call. = FALSE)
  }
}

# ------------------------------------------------------------------------------
# 2) Read and append all files
# ------------------------------------------------------------------------------
# Build an index of all expected raw file paths
index <- expand.grid(region = regions, sex = sexes, stringsAsFactors = FALSE)
index$path <- mapply(
  function(r, s) file.path(dir_raw, sprintf("jmd_%s_%sltper_1x1.txt", r, s)),
  index$region,
  index$sex
)

# Fail early if any expected raw files are missing
if (any(!file.exists(index$path))) {
  stop(
    "Some expected raw files are missing:\n",
    paste(index$path[!file.exists(index$path)], collapse = "\n"),
    call. = FALSE
  )
}

# Read, clean, and append all life table files into one data frame
# This block loops over every region × sex file, reads and cleans each one, and stacks them all into a single data frame called data
# do.call is used to call the rbind function on the list of data frames returned by Map (because that's what rbind expects)
clean_df <- do.call(rbind, Map(function(path, r, s) { # Map collects the outputs of the function for each combination of path, r, s and do.call(rbind, ...) stacks them into one data frame.
  
  # Read life table:
  #   - line 1: metadata/title
  #   - line 2: blank
  #   - line 3: column headers
  df <- utils::read.table(
    path,
    header = TRUE,
    skip = 2,
    sep = "",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  # Clean column names to be lowercase and Stata-safe
  nm <- tolower(names(df))
  nm <- gsub("[^a-z0-9_]", "_", nm) # replace Any character that is NOT a lowercase letter, digit, or underscore with underscore
  nm <- gsub("^([0-9])", "_\\1", nm) # add underscore prefix to names starting with a digit
  names(df) <- make.unique(nm, sep = "_") # ensure unique names by appending _1, _2, etc. as needed
  
  # Convert age from character to integer (e.g. "110+" -> 110)
  if ("age" %in% names(df)) { # if there is a column called age in the data frame
    df$age <- as.integer(gsub("\\+", "", df$age)) 
  }
  
  # Extract region name from the first line of the file
  # Examples:
  #   "01.Hokkaido.ken, ..." -> "Hokkaido"
  #   "13.Tokyo.to, ..."    -> "Tokyo"
  #   "00.Japan, ..."       -> "Japan"
  hdr  <- readLines(path, n = 1, warn = FALSE)
  name <- trimws(sub(",.*$", "", hdr)) # remove everything after the first comma
  name <- sub("^[0-9]{2}\\.", "", name) # remove leading region code and dot
  name <- sub("\\.(ken|fu|to)$", "", name, ignore.case = TRUE) # remove suffixes like .ken, .fu, .to
  
  # Add identifiers
  df$geocode <- r
  df$sex     <- c(m = 1L, f = 2L, b = 3L)[s]
  df$name    <- name
  
  df
}, index$path, index$region, index$sex))


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
  var = c("year", "sex", "age", "lx","lx_1","geocode","name"),
  var_label = c(
    "Calendar year",
    "Sex",
    "Age in years",
    "lx (number of survivors at age x)",
    "Lx, number of person-years lived between ages x and x+1",
    "Code for region",
    "Name of region"
  ),
  notes = c(
    "",
    "1=male, 2=female",
    "Single-year age. 110+ stored as 110",
    "",
    "Names are all lowercase, therefore _1 was added to Lx","",""
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
