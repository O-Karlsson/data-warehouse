# R/build_hld_life_tables_ITA_GBR_DEU.R
#
# Download + lightly clean HLD life tables for GBR/ITA/DEU.
# Raw:    data/raw/HLD/life tables/<country>/<country>.zip   (ZIP only)
# Cleaned data outputs:
#         data/cleaned/HLD/life tables/<country>/data.(parquet|csv|dta)
#
# Cleaning:
#   - Make names Stata-safe (remove (), -, then sanitize)
#   - Rename: m(x) q(x) l(x) d(x) L(x) T(x) e(x) e(x)Orig
#         -> mx  qx  lx  dx  lx_1 tx  ex  exOrig
#   - Coerce numeric-like character columns to numeric

source("R/utils.R")

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
  library(haven)
  library(purrr)
  library(tibble)
  library(stringr)
})

COUNTRIES <- tibble::tribble(
  ~iso3, ~country_name, ~url,
  "GBR", "United Kingdom", "https://www.lifetable.de/File/GetDocument/data/GBR/GBR.zip",
  "ITA", "Italy",           "https://www.lifetable.de/File/GetDocument/data/ITA/ITA.zip",
  "DEU", "Germany",         "https://www.lifetable.de/File/GetDocument/data/DEU/DEU.zip"
)

RAW_BASE <- file.path("data", "raw", "HLD", "life tables")
OUT_BASE <- file.path("data", "cleaned", "HLD", "life tables")

# -------------------------
# Helpers
# -------------------------

download_if_needed <- function(url, dest) {
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  
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

# Read a delimited file, trying a few common combinations
read_any_delim <- function(path) {
  try_delim <- function(delim, dec_mark = ".") {
    readr::read_delim(
      file = path,
      delim = delim,
      show_col_types = FALSE,
      progress = FALSE,
      locale = readr::locale(decimal_mark = dec_mark, grouping_mark = ","),
      na = c("", "NA", "NaN", ".", ".."),
      quote = "\""
    )
  }
  
  candidates <- list(
    list(delim = "\t", dec = "."),
    list(delim = ";",  dec = ","),
    list(delim = ";",  dec = "."),
    list(delim = ",",  dec = ".")
  )
  
  best <- NULL
  best_ncol <- -1
  best_nrow <- -1
  
  for (cand in candidates) {
    df <- tryCatch(try_delim(cand$delim, cand$dec), error = function(e) NULL)
    if (is.null(df)) next
    nc <- ncol(df); nr <- nrow(df)
    if (nc > best_ncol || (nc == best_ncol && nr > best_nrow)) {
      best <- df
      best_ncol <- nc
      best_nrow <- nr
    }
  }
  
  if (is.null(best) || best_ncol <= 1) {
    stop("Could not reliably parse file: ", path, call. = FALSE)
  }
  
  best
}

# Remove ONLY: ( ) -
# Then ensure Stata validity without introducing dots.
make_stata_names <- function(nm) {
  nm2 <- nm
  
  # Remove the three requested characters (do NOT replace)
  nm2 <- gsub("[()\\-]", "", nm2)
  
  # Then make remaining names Stata-safe:
  # letters/digits/underscore only
  nm2 <- gsub("[^A-Za-z0-9_]", "_", nm2)
  nm2 <- gsub("_+", "_", nm2)
  nm2 <- gsub("^_+", "_", nm2)
  nm2 <- ifelse(grepl("^[A-Za-z_]", nm2), nm2, paste0("_", nm2))
  nm2 <- gsub("_+$", "", nm2)
  
  nm2 <- substr(nm2, 1, 32)
  nm2 <- make.unique(nm2, sep = "_")
  nm2 <- substr(nm2, 1, 32)
  
  nm2
}

rename_hld_cols <- function(df) {
  # after name cleaning, "m(x)" becomes "mx" already if present as m(x),
  # but we keep an explicit mapping to be safe when parentheses remain in source headers.
  map <- c(
    "m(x)"     = "mx",
    "q(x)"     = "qx",
    "l(x)"     = "lx",
    "d(x)"     = "dx",
    "L(x)"     = "lx_1",
    "T(x)"     = "tx",
    "e(x)"     = "ex",
    "e(x)Orig" = "exOrig"
  )
  
  nms <- names(df)
  idx <- match(nms, names(map))
  nms[!is.na(idx)] <- unname(map[idx[!is.na(idx)]])
  names(df) <- nms
  df
}

looks_numeric <- function(x) {
  if (!is.character(x)) return(FALSE)
  y <- trimws(gsub(",", "", x, fixed = TRUE))
  y[y == ""] <- NA_character_
  y <- y[!is.na(y)]
  if (length(y) == 0) return(FALSE)
  all(grepl("^[-+]?(\\d+\\.?\\d*|\\d*\\.?\\d+)([eE][-+]?\\d+)?$", y))
}

coerce_numeric_columns <- function(df) {
  df %>%
    mutate(across(
      where(~ is.character(.x) && looks_numeric(.x)),
      ~ as.numeric(gsub(",", "", trimws(.x)))
    ))
}

# Extract zip to temp, read files, then temp is deleted
read_zip_to_df <- function(zip_path) {
  tmp <- file.path(tempdir(), paste0("hld_", as.integer(runif(1, 1e7, 9e7))))
  dir.create(tmp, recursive = TRUE, showWarnings = FALSE)
  
  on.exit(unlink(tmp, recursive = TRUE, force = TRUE), add = TRUE)
  
  utils::unzip(zipfile = zip_path, exdir = tmp)
  
  files <- list.files(tmp, recursive = TRUE, full.names = TRUE)
  files <- files[!grepl("\\.zip$", files, ignore.case = TRUE)]
  
  keep <- grepl("\\.(txt|tsv|csv|dat)$", files, ignore.case = TRUE)
  if (any(keep)) files <- files[keep]
  
  if (length(files) == 0) stop("No data files found inside zip: ", zip_path, call. = FALSE)
  
  dfs <- purrr::map(files, function(f) {
    df <- read_any_delim(f)
    df <- df %>% mutate(source_file = basename(f), .before = 1)
    df
  })
  
  bind_rows(dfs)
}

write_sources_md <- function(raw_dir, country, url) {
  sources_md <- file.path(raw_dir, "SOURCES.md")
  if (file.exists(sources_md)) return(invisible(TRUE))
  
  writeLines(
    c(
      "Source: Human Lifetable Database (HLD) / lifetable.de",
      paste0("Country: ", country),
      paste0("Downloaded from: ", url),
      paste0("This file was created on: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"))
    ),
    sources_md
  )
  invisible(TRUE)
}

write_dataset_notes <- function(out_dir, country) {
  
  dataset_notes <- c(
    sprintf("This dataset contains 1x1 period life tables from the Human Lifetable Database (HLD) for %s.", country),
    "Source data are distributed via lifetable.de (Human Lifetable Database).",
    "Raw inputs are stored as the original ZIP file only (no extracted files are kept in the raw folder).",
    "Before saving, variable names are made Stata-safe by removing '(', ')', and '-' and then sanitizing other illegal characters (to ensure Stata compatibility).",
    "Life table columns are renamed to standard short forms: m(x)->mx, q(x)->qx, l(x)->lx, d(x)->dx, L(x)->lx_1, T(x)->tx, e(x)->ex, e(x)Orig->exOrig.",
    "Columns that look numeric are coerced to numeric where possible.",
    "The column `source_file` indicates which extracted file each row came from."
  )
  
  notes_path <- file.path(out_dir, "DATASET_NOTES.md")
  
  notes_lines <- c(
    "# Dataset notes",
    "",
    sprintf("_Generated on %s_", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    "",
    paste0("- ", dataset_notes)
  )
  
  writeLines(notes_lines, notes_path)
  message("Wrote dataset notes: ", notes_path)
  
  invisible(TRUE)
}

# -------------------------
# Run
# -------------------------

dir.create(RAW_BASE, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)

for (i in seq_len(nrow(COUNTRIES))) {
  iso3    <- COUNTRIES$iso3[i]
  country <- COUNTRIES$country_name[i]
  url     <- COUNTRIES$url[i]
  
  raw_dir <- file.path(RAW_BASE, country)
  out_dir <- file.path(OUT_BASE, country)
  
  dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  zip_path <- file.path(raw_dir, paste0(iso3, ".zip"))
  
  # Raw folder contains ZIP only
  download_if_needed(url, zip_path)
  write_sources_md(raw_dir, country, url)
  
  # Read from zip by extracting to temp only
  df_raw <- read_zip_to_df(zip_path)
  
  # Make names Stata-safe FIRST, and use that df for everything
  names(df_raw) <- make_stata_names(names(df_raw))
  
  # Now apply the HLD renames (works even if original headers had parentheses)
  df <- df_raw %>%
    rename_hld_cols() %>%
    coerce_numeric_columns() %>%
mutate(country = country,
       iso3    = iso3,
       .before = 1)  
  arrow::write_parquet(df, file.path(out_dir, "data.parquet"))
  readr::write_csv(df, file.path(out_dir, "data.csv"))
  haven::write_dta(df, file.path(out_dir, "data.dta"))
  write_dataset_notes(out_dir, country)
  
  message("Wrote cleaned outputs to: ", out_dir)
}

message("\nDone.")
