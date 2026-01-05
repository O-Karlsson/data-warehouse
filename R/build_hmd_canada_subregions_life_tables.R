# R/build_hmd_canada_subregions_life_tables.R
#
# PURPOSE
# -------
# Download, clean, and combine Canadian (overall + subregions) 1x1 period life tables
# into a single dataset.
#
# Differences vs Japan script:
#   - Source/URL pattern: http://www.prdh.umontreal.ca/BDLC/data/[r]/[s]ltper_1x1.txt
#   - Regions: 3-letter codes (can, ont, que, ...)
#   - Region names: parsed from header, but with Canada-specific cleanup rules

# ------------------------------------------------------------------------------
# Global constants
# ------------------------------------------------------------------------------
source("R/utils.R")

BASE_URL <- "http://www.prdh.umontreal.ca/BDLC/data"
regions <- c("can","nfl","pei","nsc","nbr","que","ont","man","sas","alb","bco","nwt","yuk")
sexes   <- c("b","m","f")   # both, male, female (site uses b/m/f)

# ------------------------------------------------------------------------------
# Directory setup
# ------------------------------------------------------------------------------
dir_raw <- file.path("data","raw","HMD","life tables","Canada")
dir_clean <- file.path("data","cleaned","HMD","life tables","Canada")
dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------------------------
# 1) Download raw files (only if missing or empty)
# ------------------------------------------------------------------------------
for (r in regions) for (s in sexes) {
  
  url  <- sprintf("%s/%s/%sltper_1x1.txt", BASE_URL, r, s)
  dest <- file.path(dir_raw, sprintf("%s_%sltper_1x1.txt", r, s))
  
  if (file.exists(dest) && file.info(dest)$size > 0) next
  
  ok <- tryCatch({
    suppressWarnings(utils::download.file(url, dest, mode = "wb", quiet = TRUE))
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
index <- expand.grid(region = regions, sex = sexes, stringsAsFactors = FALSE)
index$path <- mapply(
  function(r, s) file.path(dir_raw, sprintf("%s_%sltper_1x1.txt", r, s)),
  index$region,
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
  
  # Canada files follow the same general structure as Japan:
  #   line 1: metadata/title
  #   line 2: blank
  #   line 3: header row
  #   data: whitespace-separated
  df <- utils::read.table(
    path,
    header = TRUE,
    skip = 2,
    sep = "",
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
  
  # Clean column names to be lowercase and Stata-safe (same logic as Japan)
  nm <- tolower(names(df))
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  names(df) <- make.unique(nm, sep = "_")
  
  # Convert age from character to integer (e.g. "110+" -> 110)
  if ("age" %in% names(df)) {
    df$age <- as.integer(gsub("\\+", "", df$age))
  }
  
  # Extract region name from the first line of the file (Canada-specific cleanup)
  # Examples seen:
  #   "Canada-Ontario, ..." -> "Ontario"
  #   "Canada, ..."         -> "Canada"
  #   "CAN_YUK, ..."        -> "Yukon"   (manual cleanup for known odd case)
  hdr  <- readLines(path, n = 1, warn = FALSE)
  name <- trimws(sub(",.*$", "", hdr))
  if (grepl("^Canada-", name)) name <- sub("^Canada-", "", name)
  if (identical(name, "CAN_YUK")) name <- "Yukon"
  if (identical(name, "Canada"))  name <- "Canada"
  
  # Add identifiers (keep the same conventions as Japan where possible)
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


