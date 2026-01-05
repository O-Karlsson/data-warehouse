# R/build_hmd_usa_life_tables.R
#
# PURPOSE
# -------
# Download, clean, and combine U.S. (overall + states) 1x1 period life tables
# into a single dataset.
#
# Source:
#   Harvard Dataverse DOI: 10.7910/DVN/19WYUX
#   Direct download (zip): https://dataverse.harvard.edu/api/access/datafile/11378078
#
# Raw structure in zip:
#   States/<STATE>/<STATE>_<sex>ltper_1x1.txt
#   Nationals/USA/USA_<sex>ltper_1x1.txt
#
# Files follow the same general structure as the Canada script:
#   line 1: metadata/title
#   line 2: blank
#   line 3: header row
#   data: whitespace-separated (HMD-style)

# ------------------------------------------------------------------------------
# Global constants
# ------------------------------------------------------------------------------
source("R/utils.R")

ZIP_URL  <- "https://dataverse.harvard.edu/api/access/datafile/11378078"
ZIP_NAME <- "USStateLifetables2022.zip"

sexes <- c("b","m","f")  # both, male, female (file suffix uses b/m/f)

# ------------------------------------------------------------------------------
# Directory setup
# ------------------------------------------------------------------------------
dir_raw   <- file.path("data","raw","HMD","life tables","USA")
dir_clean <- file.path("data","cleaned","HMD","life tables","USA")
dir.create(dir_raw,   recursive = TRUE, showWarnings = FALSE)
dir.create(dir_clean, recursive = TRUE, showWarnings = FALSE)

zip_path <- file.path(dir_raw, ZIP_NAME)

# ------------------------------------------------------------------------------
# 1) Download raw zip (only if missing or empty)
# ------------------------------------------------------------------------------
if (!(file.exists(zip_path) && file.info(zip_path)$size > 0)) {
  
  ok <- tryCatch({
    suppressWarnings(utils::download.file(ZIP_URL, zip_path, mode = "wb", quiet = TRUE))
    file.exists(zip_path) && file.info(zip_path)$size > 0
  }, error = function(e) FALSE)
  
  if (!ok) {
    if (file.exists(zip_path)) unlink(zip_path)
    stop(sprintf("Download failed: %s -> %s", ZIP_URL, zip_path), call. = FALSE)
  }
}

# ------------------------------------------------------------------------------
# 2) Build an index of expected files inside the zip
# ------------------------------------------------------------------------------
zfiles <- utils::unzip(zip_path, list = TRUE)$Name

# State codes from folder names (States/<STATE>/...)
state_codes <- unique(sub("^States/([^/]+)/.*$", "\\1", zfiles[grepl("^States/[^/]+/", zfiles)]))
state_codes <- sort(state_codes[nchar(state_codes) == 2])

# Expected .txt files only (ignore duplicate .csv files)
expected_states <- unlist(lapply(state_codes, function(st) {
  sprintf("States/%s/%s_%sltper_1x1.txt", st, st, sexes)
}))
expected_usa <- sprintf("Nationals/USA/USA_%sltper_1x1.txt", sexes)

expected <- c(expected_states, expected_usa)

# Only keep those that actually exist in the zip
keep <- intersect(zfiles, expected)

if (length(keep) == 0) {
  stop(
    "No matching .txt 1x1 life table files found in the zip.\n",
    "Expected paths like States/AK/AK_mltper_1x1.txt and Nationals/USA/USA_mltper_1x1.txt.",
    call. = FALSE
  )
}

# Build an index data.frame similar to Canada script
index <- do.call(rbind, lapply(keep, function(rel) {
  if (grepl("^States/", rel)) {
    st <- sub("^States/([^/]+)/.*$", "\\1", rel)
    sx <- sub("^.*_([bmf])ltper_1x1\\.txt$", "\\1", basename(rel))
    data.frame(geocode = st, sex = sx, relpath = rel, stringsAsFactors = FALSE)
  } else {
    sx <- sub("^.*_([bmf])ltper_1x1\\.txt$", "\\1", basename(rel))
    data.frame(geocode = "USA", sex = sx, relpath = rel, stringsAsFactors = FALSE)
  }
}))

# ------------------------------------------------------------------------------
# 3) Extract the needed files to a temp folder, read, and append
# ------------------------------------------------------------------------------
tmp_dir <- tempfile("us_lt_")
dir.create(tmp_dir)
on.exit(unlink(tmp_dir, recursive = TRUE, force = TRUE), add = TRUE)

utils::unzip(zip_path, files = index$relpath, exdir = tmp_dir)

index$path <- file.path(tmp_dir, index$relpath)

if (any(!file.exists(index$path))) {
  stop(
    "Some expected extracted files are missing:\n",
    paste(index$path[!file.exists(index$path)], collapse = "\n"),
    call. = FALSE
  )
}

clean_df <- do.call(rbind, Map(function(path, g, s) {
  
  # Same read pattern as Canada:
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
  
  # Clean column names to be lowercase and Stata-safe (same logic as Canada/Japan)
  nm <- tolower(names(df))
  nm <- gsub("[^a-z0-9_]", "_", nm)
  nm <- gsub("^([0-9])", "_\\1", nm)
  names(df) <- make.unique(nm, sep = "_")
  
  # Convert age from character to integer (e.g. "110+" -> 110)
  if ("age" %in% names(df)) {
    df$age <- as.integer(gsub("\\+", "", df$age))
  }
  
  # Add identifiers (keep same conventions)
  df$geocode <- g
  df$sex     <- c(m = 1L, f = 2L, b = 3L)[s]
  
  df
}, index$path, index$geocode, index$sex))

# ------------------------------------------------------------------------------
# 4) Write outputs (parquet + dta + csv)
# ------------------------------------------------------------------------------
# *****************************************************************************
# OLD simple write (no archiving): uncomment to use (and remove code blocks below)
# arrow::write_parquet(clean_df, file.path(dir_clean, "data.parquet"))
# haven::write_dta(clean_df,     file.path(dir_clean, "data.dta"))
# readr::write_csv(clean_df,     file.path(dir_clean, "data.csv"))
# *****************************************************************************

# *****************************************************************************
# Build script output + archiving pattern (same as Canada script)
arch_dir <- file.path(dir_clean, "_archive")
out_parquet_final <- file.path(dir_clean, paste0("data", ".parquet"))

out_parquet_tmp <- tempfile(fileext = ".parquet")
arrow::write_parquet(clean_df, out_parquet_tmp)

res <- archive_and_promote(
  final_path  = out_parquet_final,
  tmp_path    = out_parquet_tmp,
  archive_dir = arch_dir
)

if (isTRUE(res$changed)) {
  tag <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
  
  # Archive old dta/csv if they exist
  if (file.exists(file.path(dir_clean, "data.dta"))) {
    file.copy(file.path(dir_clean, "data.dta"),
              file.path(arch_dir, paste0("data_", tag, ".dta")),
              overwrite = FALSE)
  }
  if (file.exists(file.path(dir_clean, "data.csv"))) {
    file.copy(file.path(dir_clean, "data.csv"),
              file.path(arch_dir, paste0("data_", tag, ".csv")),
              overwrite = FALSE)
  }
}

haven::write_dta(clean_df, file.path(dir_clean, "data.dta"))
readr::write_csv(clean_df, file.path(dir_clean, "data.csv"))
# *****************************************************************************

# ------------------------------------------------------------------------------
# Dataset-level notes (general documentation)
# ------------------------------------------------------------------------------
dataset_notes <- c(
  "This dataset contains 1x1 period life tables for USA total and all U.S. states.",
  "Source: Harvard Dataverse DOI: 10.7910/DVN/19WYUX (zip downloaded via Dataverse API datafile id 11378078).",
  "Raw input is a zip containing HMD-style text files with 2 header lines (metadata + blank) followed by a header row.",
  "geocode is the state postal code for states (AK, AL, ...) and 'USA' for national.",
  "sex is coded as 1=male, 2=female, 3=both (from file suffix m/f/b)."
)

notes_path <- file.path(dir_clean, "DATASET_NOTES.md")
notes_lines <- c(
  "# Dataset notes",
  "",
  sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
  "",
  paste("- ", dataset_notes)
)

writeLines(notes_lines, notes_path)
message("Wrote dataset notes: ", notes_path)

# ------------------------------------------------------------------------------
# Variable dictionary (edit var_label / notes here)
# ------------------------------------------------------------------------------
# (Keep this similar to Canada: list the main identifiers + any special-case vars)
var_dict <- data.frame(
  var = c("year", "sex", "age", "geocode"),
  var_label = c(
    "Calendar year",
    "Sex",
    "Age in years",
    "Code for region (state postal code or USA)"
  ),
  notes = c(
    "",
    "1=male, 2=female, 3=both",
    "Single-year age. 110+ stored as 110",
    ""
  ),
  stringsAsFactors = FALSE
)

vars_path <- file.path(dir_clean, "variables_info.csv")
write.csv(var_dict, vars_path, row.names = FALSE, na = "")
message("Wrote variable dictionary: ", vars_path)

# ------------------------------------------------------------------------------
# Emit documentation warnings at the very end (same pattern as Canada)
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

message("Done. Clean outputs in: ", dir_clean)
